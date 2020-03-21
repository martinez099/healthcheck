import functools
import math

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import GB, to_gb, to_kops, redis_ping


class Database(BaseCheckSuite):
    """Databases - configuration, sizing and usage"""

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        super().__init__()
        self.api = ApiFetcher.instance(_config)

    def check_oss_api(self):
        """check for OSS cluster API of each database"""
        bdbs = self.api.get('bdbs')
        kwargs = {}
        for bdb in filter(lambda x: x['oss_cluster'], bdbs):
            kwargs[bdb['name']] = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'

        return all(kwargs.values()) if kwargs.values() else '', {'reason': 'no OSS cluster API enabled database found'}

    def check_shards_placement(self):
        """check for dense shards placement of each database"""
        nodes = self.api.get('nodes')
        bdbs = self.api.get('bdbs')
        kwargs = {}

        dense_bdbs = filter(lambda x: x['shards_placement'] == 'dense', bdbs)
        if not dense_bdbs:
            return '', {'reason': 'no dense shards placement database found'}

        for bdb in dense_bdbs:
            if bdb['proxy_policy'] != 'single':
                kwargs[bdb['name']] = "proxy policy set to '{}' instead of 'single'".format(bdb['proxy_policy'])
                continue

            internal_endpoints = list(filter(lambda e: e['addr_type'] == 'internal', bdb['endpoints']))
            if len(internal_endpoints) > 1:
                kwargs[bdb['name']] = "multiple internal endpoints found"
                continue

            endpoint_addrs = internal_endpoints[0]['addr']
            if len(endpoint_addrs) > 1:
                kwargs[bdb['name']] = "multiple addresses for internal endpoint found"
                continue

            endpoint_nodes = list(filter(lambda n: n['addr'] == endpoint_addrs[0] and n['uid'], nodes))
            if len(endpoint_nodes) > 1:
                kwargs[bdb['name']] = "multiple nodes for endpoint found"
                continue

            master_shards = filter(lambda s: s['bdb_uid'] == bdb['uid'] and s['role'] == 'master', self.api.get('shards'))
            shards_not_on_endpoint = filter(lambda s: int(s['node_uid']) != endpoint_nodes[0]['uid'], master_shards)
            result = list(map(lambda r: 'shard:{}'.format(r['uid']), shards_not_on_endpoint))
            if result:
                kwargs[bdb['name']] = result

        return not any(kwargs.values()), kwargs

    def check_replica_sources(self):
        """check replicaOf sources"""
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

    def check_crdt_sources(self):
        """check CRDB sources"""
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

    def check_endpoints(self):
        """check database endpoints"""
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

    def check_config(self, **_params):
        """check database configuration"""
        bdbs = self.api.get('bdbs')
        results = []

        if not _params:
            for bdb in bdbs:
                results.append((None, {'uid': bdb['uid'], 'memory limit': f'{to_gb(bdb["memory_size"])} GB',
                                       'master shards': bdb['shards_count'], 'HA': bdb['replication'],
                                       'OSS cluster': bdb['oss_cluster'], 'CRDB': bdb['crdt']},
                                f"""get configuration of '{bdb['name']}'"""))

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

    def check_throughput(self):
        """check throughput of each shard"""
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

                kwargs[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = '{}/{}/{}/{} Kops/sec'.format(
                    to_kops(minimum), to_kops(average), to_kops(maximum), to_kops(std_dev))

        return [(not results[bdb['name']], kwargs[bdb['name']], f"check throughput for '{bdb['name']}' (min/avg/max/dev)")
                for bdb in bdbs]

    def check_memory_usage(self):
        """check memory usage of each shard"""
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

        return [(not results[bdb['name']], kwargs[bdb['name']], f"check memory usage for '{bdb['name']}' (min/avg/max/dev)")
                for bdb in bdbs]
