import functools
import math

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import GB, to_gb, to_kops, redis_ping


class Databases(BaseCheckSuite):
    """
    Check configuration, status and usage of all databases.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.api = ApiFetcher.instance(_config)

    def check_databases_config_001(self, _params):
        """DC-001: Check database configuration.

        Calls '/v1/bdbs' from API and compares the values against the passed parameters.
        See API doc for a description of possible values.
        If no parameters are passed, just outputs a subset of configuration values for each database.

        If this check fails, adapt your database configuration in the UI or per REST-API.

        :param _params: A dict with database configuration values. See 'parameter_maps/databases/check_config' for examples.
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        results = []

        if not _params:
            for bdb in bdbs:
                results.append((None, {'uid': bdb['uid'], 'memory limit': f'{to_gb(bdb["memory_size"])} GB',
                                       'master shards': bdb['shards_count'], 'HA': bdb['replication'],
                                       'OSS cluster': bdb['oss_cluster'], 'CRDB': bdb['crdt']},
                                f"DC-001: Get configuration of '{bdb['name']}'."))

        else:
            for bdb in bdbs:
                values = dict(_params['__default__'])
                if bdb['name'] in _params:
                    values.update(_params[bdb['name']])

                kwargs = {}
                for k, v in values.items():
                    result = v == bdb[k]
                    if not result:
                        kwargs[k] = bdb[k]

                results.append((not bool(kwargs), kwargs, f"""check configuration of '{bdb['name']}'"""))

        return results

    def check_databases_config_002(self, _params):
        """DC-002: Check for OSS cluster API of each database.

        Calls '/v1/bdbs' from API and checks databases which are 'oss_cluster' enabled if their
        'shards_placement' is 'sparse' and their 'proxy_policy' is set to 'all-master-shards'.

        If this check fails, adapt your database configuration through `rladmin`.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        kwargs = {}
        for bdb in filter(lambda x: x['oss_cluster'], bdbs):
            kwargs[bdb['name']] = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'

        return all(kwargs.values()) if kwargs.values() else '', kwargs

    def check_databases_config_003(self, _params):
        """DC-003: Check for dense shards placement of each database.

        Calls 'v1/bdbs' from API and checks databases which have 'shards_placement' set to 'dense'
        if their master shards are on the same node as their single proxy.

        If this check fails, move all master shards to the node where the proxy runs.

        :param _params: None
        :returns: result
        """
        nodes = self.api.get('nodes')
        bdbs = self.api.get('bdbs')
        kwargs = {}

        dense_bdbs = filter(lambda x: x['shards_placement'] == 'dense', bdbs)
        if not dense_bdbs:
            return '', kwargs

        for bdb in dense_bdbs:
            if bdb['proxy_policy'] != 'single':
                kwargs[bdb['name']] = "proxy policy set to '{}' instead of 'single'".format(bdb['proxy_policy'])
                continue

            endpoint_nodes = list(filter(lambda node: node['addr'] == bdb['endpoints'][0]['addr'][0] and node['uid'], nodes))
            if len(endpoint_nodes) < 1:
                kwargs[bdb['name']] = f"no endpoint node found"
                continue

            master_shards = filter(lambda shard: shard['bdb_uid'] == bdb['uid'] and shard['role'] == 'master', self.api.get('shards'))
            shards_not_on_endpoint = filter(lambda shard: int(shard['node_uid']) != endpoint_nodes[0]['uid'], master_shards)
            result = list(map(lambda shard: 'shard:{}'.format(shard['uid']), shards_not_on_endpoint))
            if result:
                kwargs[bdb['name']] = result

        return not any(kwargs.values()), kwargs

    def check_databases_status_001(self, _params):
        """DS-001: Check replicaOf sources.

        Calls '/v1/bdbs' from API and checks databases which have a 'replica_sources' entry if their 'status' is 'in-sync'.

        If this check fails, try investigating the network link between the failing databases.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        kwargs = {}

        for bdb in bdbs:
            replica_sources = bdb['replica_sources']
            if replica_sources:
                kwargs[bdb['name']] = {
                    'replica_sync': bdb['replica_sync']
                }
            for replica_source in replica_sources:
                address = replica_source['uri'].split('@')[1]
                kwargs[bdb['name']][address] = {
                    'status': replica_source['status'],
                    'lag': replica_source['lag'],
                    'compression': replica_source['compression']
                }

        return all(filter(lambda x: x[0] == 'in-sync',
                          map(lambda x: list(x.values()), kwargs.values()))) if kwargs else '', kwargs

    def check_databases_status_002(self, _params):
        """DS-002: Check CRDB sources.

        Calls '/v1/bdbs' from API and checks databases which have a 'crdt_sources' entry if their 'status' is 'in-sync'.

        If this check fails, try investigating the network link between the failing databases.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        kwargs = {}

        for bdb in bdbs:
            crdt_sources = bdb['crdt_sources']
            if crdt_sources:
                kwargs[bdb['name']] = {
                    'crdt_sync': bdb['crdt_sync']
                }
            for crdt_source in crdt_sources:
                address = crdt_source['uri'].split('@')[1]
                kwargs[bdb['name']][address] = {
                    'status': crdt_source['status'],
                    'lag': crdt_source['lag'],
                    'compression': crdt_source['compression']
                }

        return all(filter(lambda x: x[0] == 'in-sync',
                          map(lambda x: list(x.values()), kwargs.values()))) if kwargs else '', kwargs

    def check_databases_status_003(self, _params):
        """DS-003: Check database endpoints.

        Calls '/v1/bdbs' from API and sends a Redis PING to each endpoint and compares the response to 'PONG'.

        If this check fails, try investigating the network connection to the endpoint.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        kwargs = {}

        for bdb in bdbs:
            endpoints = bdb['endpoints']
            if len(endpoints) > 1:
                endpoint = list(filter(lambda x: x['addr_type'] == 'external', endpoints))[0]
            else:
                endpoint = endpoints[0]

            result = redis_ping(endpoint['addr'][0], endpoint['port'])
            kwargs[endpoint['dns_name']] = result

        return all(v is True for v in kwargs.values()), kwargs

    def check_databases_usage_001(self, _params):
        """DU-001: Check throughput of each shard.

        Calls '/v1/bdbs' from API and calculates min/avg/max/dev for 'total_req' of each shard.
        It compares the maximum value to Redis Labs recommended upper limitsi, i.e. 25 Kops.

        If this check fails, try adding more shards or investigate the key distribution.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        kwargs = {}
        results = {}

        for bdb in bdbs:
            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')
                ints = shard_stats['intervals']

                # calculate minimum
                minimum = min([i['total_req'] for i in filter(lambda i: i.get('total_req'), ints)])

                # calculate average
                total_reqs = list(filter(lambda i: i.get('total_req'), ints))
                sum_total_req = sum([i['total_req'] for i in total_reqs])
                average = sum_total_req / len(total_reqs)

                # calculate maximum
                maximum = max([i['total_req'] for i in filter(lambda i: i.get('total_req'), ints)])

                # calculate std deviation
                q_sum = functools.reduce(lambda x, y: x + pow(y['total_req'] - average, 2), total_reqs, 0)
                std_dev = math.sqrt(q_sum / len(total_reqs))

                if bdb['bigstore']:
                    result = maximum > 5000
                elif bdb['crdt_sync'] != 'disabled':
                    result = maximum > 17500
                else:
                    result = maximum > 25000
                results[bdb['name']] = result

                if bdb['name'] not in kwargs:
                    kwargs[bdb['name']] = {}

                kwargs[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = '{}/{}/{}/{} Kops'.format(
                    to_kops(minimum), to_kops(average), to_kops(maximum), to_kops(std_dev))

        return [(not results[bdb['name']], kwargs[bdb['name']], f"DU-001: Check throughput of '{bdb['name']}' (min/avg/max/dev).")
                for bdb in bdbs]

    def check_databases_usage_002(self, _params):
        """DU-002: Check memory usage of each shard.

        Calls '/v1/bdbs' from API and calculates min/avg/max/dev for 'used_memory' of each shard.
        It compares the maximum value to Redis Labs recommended upper limits, i.e. 25 GB.

        If this check fails, try adding more shards or investigate the key distribution.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        kwargs = {}
        results = {}

        for bdb in bdbs:
            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')
                ints = shard_stats['intervals']

                # calculate minimum
                minimum = min([i['used_memory'] for i in filter(lambda i: i.get('used_memory'), ints)])

                # calculate average
                used_memories = list(filter(lambda x: x.get('used_memory'), ints))
                sum_total_ram_usage = sum([i['used_memory'] for i in used_memories])
                average = sum_total_ram_usage / len(used_memories)

                # calculate maximum
                maximum = max([i['used_memory'] for i in filter(lambda i: i.get('used_memory'), ints)])

                # calculate std deviation
                q_sum = functools.reduce(lambda x, y: x + pow(y['used_memory'] - average, 2), used_memories, 0)
                std_dev = math.sqrt(q_sum / len(used_memories))

                if bdb['bigstore']:
                    result = maximum > (50 * GB)
                else:
                    result = maximum > (25 * GB)
                results[bdb['name']] = result

                if bdb['name'] not in kwargs:
                    kwargs[bdb['name']] = {}

                kwargs[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = '{}/{}/{}/{} GB'.format(
                    to_gb(minimum), to_gb(average), to_gb(maximum), to_gb(std_dev))

        return [(not results[bdb['name']], kwargs[bdb['name']], f"DU-002: Check memory usage of '{bdb['name']}' (min/avg/max/dev).")
                for bdb in bdbs]
