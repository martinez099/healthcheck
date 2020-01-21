from healthcheck.check_suites.base_suite import BaseCheckSuite, load_params
from healthcheck.common_funcs import GB, to_gb, to_kops


class BdbChecks(BaseCheckSuite):
    """Databases"""

    def __init__(self, _config):
        super().__init__(_config)
        self.params = load_params('databases')

    def _check_connectivity(self):
        self._check_api_connectivity()

    def check_oss_api(self, *_args, **_kwargs):
        """check for OSS cluster API"""
        bdbs = self.api.get('bdbs')
        kwargs = {}
        for bdb in filter(lambda x: x['oss_cluster'], bdbs):
            kwargs[bdb['name']] = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'

        return all(kwargs.values()) if kwargs.values() else '', {'reason': 'not activated'}

    def check_shards_placement(self, *_args, **_kwargs):
        """check for dense shards placement"""
        nodes = self.api.get('nodes')
        bdbs = self.api.get('bdbs')
        kwargs = {}

        for bdb in filter(lambda x: x['shards_placement'] == 'dense', bdbs):
            if bdb['proxy_policy'] != 'single':
                kwargs[bdb['name']] = "proxy policy set to '{}'".format(bdb['proxy_policy'])
                continue

            endpoints = list(filter(lambda e: e['addr_type'] == 'internal', bdb['endpoints']))
            if len(endpoints) > 1:
                kwargs[bdb['name']] = "multiple internal endpoints found"
                continue

            endpoint_addrs = endpoints[0]['addr']
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

    def check_bdbs(self, *_args, **_kwargs):
        """check database configuration"""
        bdbs = self.api.get('bdbs')
        results = []

        if not _kwargs:
            for bdb in bdbs:
                results.append((None, bdb, f"""get configuration of '{bdb['name']}'"""))

        else:

            for bdb in bdbs:
                values = dict(_kwargs['__default__'])
                if bdb['name'] in _kwargs:
                    values.update(_kwargs[bdb['name']])

                kwargs = {}
                for k, v in values.items():
                    result = v == bdb[k]
                    if not result:
                        kwargs[k] = bdb[k]

                results.append((not bool(kwargs), kwargs, f"""check configuration of '{bdb['name']}'"""))

        return results

    def check_cpu_usage(self, *_args, **_kwargs):
        """check maximum throughput"""
        stats = self.api.get('shards/stats')
        bdb_names = self.api.get_values('bdbs', 'name')
        kwargs = {}
        results = {}

        for stat_idx in range(0, len(stats)):
            uid = stats[stat_idx]['uid']
            role = stats[stat_idx]['role']
            bdb_uid = self.api.get(f'shards/{uid}')['bdb_uid']
            bdb_name = self.api.get(f'bdbs/{bdb_uid}')['name']
            bigstore = self.api.get(f'bdbs/{bdb_uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{bdb_uid}')['crdt_sync'] != 'disabled'
            if bdb_name not in kwargs:
                kwargs[bdb_name] = {}

            # calculate maximum throughput
            max_total_requests = max([i['total_req'] for i in filter(lambda x: x.get('total_req'), stats[stat_idx]['intervals'])])

            if bigstore:
                result = max_total_requests > 5000
            elif crdb:
                result = max_total_requests > 17500
            else:
                result = max_total_requests > 25000
            results[bdb_name] = result
            kwargs[bdb_name][f'shard:{uid} ({role})'] = '{}K ops/sec'.format(to_kops(max_total_requests))

        return [(not results[bdb_name], kwargs[bdb_name], f"check maximum throughput for '{bdb_name}'") for bdb_name in bdb_names]

    def check_ram_usage(self, *_args, **_kwargs):
        """check maximum RAM usage"""
        stats = self.api.get('shards/stats')
        bdb_names = self.api.get_values('bdbs', 'name')
        kwargs = {}
        results = {}

        for stat_idx in range(0, len(stats)):
            uid = stats[stat_idx]['uid']
            role = stats[stat_idx]['role']
            bdb_uid = self.api.get(f'shards/{uid}')['bdb_uid']
            bdb_name = self.api.get(f'bdbs/{bdb_uid}')['name']
            bigstore = self.api.get(f'bdbs/{bdb_uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{bdb_uid}')['crdt_sync'] != 'disabled'
            if bdb_name not in kwargs:
                kwargs[bdb_name] = {}

            # calculate maximum RAM usage
            max_ram_usage = max([i['used_memory'] for i in filter(lambda x: x.get('used_memory'), stats[stat_idx]['intervals'])])

            if bigstore:
                result = max_ram_usage > (50 * GB)
            else:
                result = max_ram_usage > (25 * GB)
            results[bdb_name] = result
            kwargs[bdb_name][f'shard:{uid} ({role})'] = '{} GB'.format(to_gb(max_ram_usage))

        return [(not results[bdb_name], kwargs[bdb_name], f"check maximum memory usage for '{bdb_name}'") for bdb_name in bdb_names]

    def check_cpu_balance(self, *_args, **_kwargs):
        """get average CPU usage"""
        results = []
        bdbs = self.api.get('bdbs')

        for bdb in bdbs:

            kwargs = {}
            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')
                ints = shard_stats['intervals']

                # calculate average througput
                total_reqs = list(filter(lambda x: x.get('total_req'), ints))
                sum_total_req = sum([i['total_req'] for i in total_reqs])
                avg_total_req = sum_total_req/len(total_reqs)

                kwargs = {f'shard:{shard_uid} ({shard_stats["role"]})': '{}K ops/sec'.format(to_kops(avg_total_req))}

            results.append((None, kwargs, f'get average throughput of \'{bdb["name"]}\''))

        return results

    def check_ram_balance(self, *_args, **_kwargs):
        """get average RAM usage"""
        results = []
        bdbs = self.api.get('bdbs')

        for bdb in bdbs:

            kwargs = {}
            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')
                ints = shard_stats['intervals']

                # calculate average memory usage
                total_reqs = list(filter(lambda x: x.get('used_memory'), ints))
                sum_total_req = sum([i['used_memory'] for i in total_reqs])
                avg_total_req = sum_total_req / len(total_reqs)

                kwargs = {f'shard:{shard_uid} ({shard_stats["role"]})': '{} GB'.format(to_gb(avg_total_req))}

            results.append((None, kwargs, f'get average RAM usage of \'{bdb["name"]}\''))

        return results
