from healthcheck.api_fetcher import ApiFetcher
from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import calc_usage, GB, to_gb, to_kops, redis_ping


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

        Remedy: Adapt your database configuration in the UI or per REST-API.

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

                info = {}
                for k, v in values.items():
                    result = v == bdb[k]
                    if not result:
                        info[k] = bdb[k]

                results.append((not bool(info), info, f"""check configuration of '{bdb['name']}'"""))

        return results

    def check_databases_config_002(self, _params):
        """DC-002: Check database endpoints.

        Calls '/v1/bdbs' from API and sends a Redis PING to each endpoint and compares the response to 'PONG'.

        Remedy: Investigate the network connection to the endpoint.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}

        for bdb in bdbs:
            endpoints = bdb['endpoints']
            if len(endpoints) > 1:
                endpoint = list(filter(lambda x: x['addr_type'] == 'external', endpoints))[0]
            else:
                endpoint = endpoints[0]

            result = redis_ping(endpoint['addr'][0], endpoint['port'])
            info[endpoint['dns_name']] = result

        return all(v is True for v in info.values()) if info else '', info

    def check_databases_config_003(self, _params):
        """DC-003: Check for OSS cluster API of each database.

        Calls '/v1/bdbs' from API and checks databases which are 'oss_cluster' enabled if their
        'shards_placement' is 'sparse' and their 'proxy_policy' is set to 'all-master-shards'.

        Remedy: Adapt your database configuration through `rladmin`.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}
        for bdb in filter(lambda x: x['oss_cluster'], bdbs):
            info[bdb['name']] = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'

        return all(info.values()) if info.values() else '', info

    def check_databases_config_004(self, _params):
        """DC-004: Check for dense shards placement of each database.

        Calls 'v1/bdbs' from API and checks databases which have 'shards_placement' set to 'dense'
        if their master shards are on the same node as their single proxy.

        Remedy: Move all master shards to the node where the proxy runs.

        :param _params: None
        :returns: result
        """
        nodes = self.api.get('nodes')
        bdbs = self.api.get('bdbs')
        info = {}

        dense_bdbs = filter(lambda x: x['shards_placement'] == 'dense', bdbs)
        if not dense_bdbs:
            return '', info

        for bdb in dense_bdbs:
            if bdb['proxy_policy'] != 'single':
                info[bdb['name']] = "proxy policy set to '{}' instead of 'single'".format(bdb['proxy_policy'])
                continue

            endpoint_nodes = list(filter(lambda node: node['addr'] == bdb['endpoints'][0]['addr'][0] and node['uid'], nodes))
            if len(endpoint_nodes) < 1:
                info[bdb['name']] = f"no endpoint node found"
                continue

            master_shards = filter(lambda shard: shard['bdb_uid'] == bdb['uid'] and shard['role'] == 'master', self.api.get('shards'))
            shards_not_on_endpoint = filter(lambda shard: int(shard['node_uid']) != endpoint_nodes[0]['uid'], master_shards)
            result = list(map(lambda shard: 'shard:{}'.format(shard['uid']), shards_not_on_endpoint))
            if result:
                info[bdb['name']] = result

        return not any(info.values()), info

    def check_database_config_005(self, _params):
        """DC-005: Get database modules.

        Calls 'v1/bdbs' from API and outputs loaded Redis modules.

        :param _params: None
        :returns: result
        """
        return None, {bdb['name']: bdb['module_list'] or None for bdb in self.api.get('bdbs')}

    def check_databases_status_001(self, _params):
        """DS-001: Check replicaOf sources.

        Calls '/v1/bdbs' from API and checks databases which have a 'replica_sources' entry if their 'status' is 'in-sync'.

        Remedy: Investigate the network link between the failing databases.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}

        for bdb in bdbs:
            replica_sources = bdb['replica_sources']
            if replica_sources:
                info[bdb['name']] = {
                    'replica_sync': bdb['replica_sync']
                }
            for replica_source in replica_sources:
                address = replica_source['uri'].split('@')[1]
                info[bdb['name']][address] = {
                    'status': replica_source['status'],
                    'lag': replica_source['lag'],
                    'compression': replica_source['compression']
                }

        return all(filter(lambda x: x[0] == 'in-sync',
                          map(lambda x: list(x.values()), info.values()))) if info else '', info

    def check_databases_status_002(self, _params):
        """DS-002: Check CRDB sources.

        Calls '/v1/bdbs' from API and checks databases which have a 'crdt_sources' entry if their 'status' is 'in-sync'.

        Remedy: Investigate the network link between the failing databases.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}

        for bdb in bdbs:
            crdt_sources = bdb['crdt_sources']
            if crdt_sources:
                info[bdb['name']] = {
                    'crdt_sync': bdb['crdt_sync']
                }
            for crdt_source in crdt_sources:
                address = crdt_source['uri'].split('@')[1]
                info[bdb['name']][address] = {
                    'status': crdt_source['status'],
                    'lag': crdt_source['lag'],
                    'compression': crdt_source['compression']
                }

        return all(filter(lambda x: x[0] == 'in-sync',
                          map(lambda x: list(x.values()), info.values()))) if info else '', info

    def check_databases_status_003(self, _params):
        """DS-003: Check database alerts.

        Calls '/v1/bdbs/alerts' from API and outputs triggered alerts.

        Remedy: Investigate triggered alerts by checking log files.

        :param _params: None
        :returns: result
        """
        alerts = self.api.get('bdbs/alerts')
        info = {}
        for uid in alerts:
            enableds = list(filter(lambda x: x[1]['state'], alerts[uid].items()))
            if enableds:
                info['db:{}'.format(uid)] = enableds

        return not info, info

    def check_databases_usage_001(self, _params):
        """DU-001: Check throughput of each database.

        Calls '/v1/bdbs' from API and calculates min/avg/max/dev for 'total_req' of each shard.
        It compares the maximum value to Redis Labs recommended upper limitsi, i.e. 25 Kops.

        Remedy: Add more shards or investigate the key distribution.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}
        results = {}

        for bdb in bdbs:
            db_stats = self.api.get(f'bdbs/stats/{bdb["uid"]}')

            minimum, average, maximum, std_dev = calc_usage(db_stats['intervals'], 'total_req')
            info[bdb['name']] = {
                'total': '{}/{}/{}/{} Kops'.format(to_kops(minimum), to_kops(average), to_kops(maximum),
                                                   to_kops(std_dev))}

            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')

                minimum, average, maximum, std_dev = calc_usage(shard_stats['intervals'], 'total_req')

                if bdb['bigstore']:
                    result = maximum > 5000
                elif bdb['crdt_sync'] != 'disabled':
                    result = maximum > 17500
                else:
                    result = maximum > 25000
                results[bdb['name']] = result

                info[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = \
                    '{}/{}/{}/{} Kops'.format(to_kops(minimum), to_kops(average), to_kops(maximum), to_kops(std_dev))

        return [(not results[bdb['name']], info[bdb['name']],
                 f"DU-001: Check throughput of '{bdb['name']}' (min/avg/max/dev).") for bdb in bdbs]

    def check_databases_usage_002(self, _params):
        """DU-002: Check memory usage of each database.

        Calls '/v1/bdbs' from API and calculates min/avg/max/dev for 'used_memory' of each shard.
        It compares the maximum value to Redis Labs recommended upper limits, i.e. 25 GB.

        Remedy: Add more shards or investigate the key distribution.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}
        results = {}

        for bdb in bdbs:
            db_stats = self.api.get(f'bdbs/stats/{bdb["uid"]}')

            minimum, average, maximum, std_dev = calc_usage(db_stats['intervals'], 'used_memory')
            info[bdb['name']] = {
                'total': '{}/{}/{}/{} GB'.format(to_gb(minimum), to_gb(average), to_gb(maximum),
                                                 to_gb(std_dev))}

            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')

                minimum, average, maximum, std_dev = calc_usage(shard_stats['intervals'], 'used_memory')

                if bdb['bigstore']:
                    result = maximum > (50 * GB)
                else:
                    result = maximum > (25 * GB)
                results[bdb['name']] = result

                info[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = \
                    '{}/{}/{}/{} GB'.format(to_gb(minimum), to_gb(average), to_gb(maximum), to_gb(std_dev))

        return [(not results[bdb['name']], info[bdb['name']],
                 f"DU-002: Check memory usage of '{bdb['name']}' (min/avg/max/dev).") for bdb in bdbs]

    def check_databases_usage_003(self, _params):
        """DU-003: Check network traffic usage of each database.

        Calls '/v1/bdbs' from API and calculates min/avg/max/dev for 'egress_bytes' and 'ingress_bytes' of each DB.

        :param _params: None
        :returns: result
        """
        bdbs = self.api.get('bdbs')
        info = {}

        for bdb in bdbs:
            db_stats = self.api.get(f'bdbs/stats/{bdb["uid"]}')

            minimum, average, maximum, std_dev = calc_usage(db_stats['intervals'], 'ingress_bytes')
            info[bdb['name']] = {
                'ingress': '{}/{}/{}/{} GB'.format(to_gb(minimum), to_gb(average), to_gb(maximum), to_gb(std_dev))
            }
            minimum, average, maximum, std_dev = calc_usage(db_stats['intervals'], 'egress_bytes')
            info[bdb['name']]['egress'] = '{}/{}/{}/{} GB'.format(to_gb(minimum), to_gb(average), to_gb(maximum), to_gb(std_dev))

        return None, info
