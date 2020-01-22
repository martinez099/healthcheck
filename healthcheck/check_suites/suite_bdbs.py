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

        return all(kwargs.values()) if kwargs.values() else '', {'reason': 'no OSS cluster API enabled database found'}

    def check_shards_placement(self, *_args, **_kwargs):
        """check for dense shards placement"""
        nodes = self.api.get('nodes')
        bdbs = self.api.get('bdbs')
        kwargs = {}

        dense_bdbs = filter(lambda x: x['shards_placement'] == 'dense', bdbs)
        if not dense_bdbs:
            return '', {'reason': 'no dense shards placement database found'}

        for bdb in dense_bdbs:
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

    def check_throughput(self, *_args, **_kwargs):
        """check throughput of each shard"""
        bdbs = self.api.get('bdbs')
        kwargs = {}
        results = {}

        for bdb in bdbs:
            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')
                ints = shard_stats['intervals']

                # calculate minimum
                min_total_req = min([i['total_req'] for i in filter(lambda i: i.get('total_req'), ints)])

                # calculate average
                total_reqs = list(filter(lambda i: i.get('total_req'), ints))
                sum_total_req = sum([i['total_req'] for i in total_reqs])
                avg_total_req = sum_total_req / len(total_reqs)

                # calculate maximum
                max_total_req = max([i['total_req'] for i in filter(lambda i: i.get('total_req'), ints)])

                if bdb['bigstore']:
                    result = max_total_req > 5000
                elif bdb['crdt_sync'] != 'disabled':
                    result = max_total_req > 17500
                else:
                    result = max_total_req > 25000
                results[bdb['name']] = result

                if bdb['name'] not in kwargs:
                    kwargs[bdb['name']] = {}

                kwargs[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = '{}/{}/{} Kops/sec'.format(
                    to_kops(min_total_req), to_kops(avg_total_req), to_kops(max_total_req))

        return [(not results[bdb['name']], kwargs[bdb['name']], f"check throughput for '{bdb['name']}' (min/avg/max)")
                for bdb in bdbs]

    def check_memory_usage(self, *_args, **_kwargs):
        """check memory usage of each shard"""
        bdbs = self.api.get('bdbs')
        kwargs = {}
        results = {}

        for bdb in bdbs:
            for shard_uid in bdb['shard_list']:
                shard_stats = self.api.get(f'shards/stats/{shard_uid}')
                ints = shard_stats['intervals']

                # calculate minimum
                min_ram_usage = min([i['used_memory'] for i in filter(lambda i: i.get('used_memory'), ints)])

                # calculate average
                total_ram_usage = list(filter(lambda x: x.get('used_memory'), ints))
                sum_total_ram_usage = sum([i['used_memory'] for i in total_ram_usage])
                avg_total_ram_usage = sum_total_ram_usage / len(total_ram_usage)

                # calculate maximum
                max_ram_usage = max([i['used_memory'] for i in filter(lambda i: i.get('used_memory'), ints)])

                if bdb['bigstore']:
                    result = max_ram_usage > (50 * GB)
                else:
                    result = max_ram_usage > (25 * GB)
                results[bdb['name']] = result

                if bdb['name'] not in kwargs:
                    kwargs[bdb['name']] = {}

                kwargs[bdb['name']][f'shard:{shard_uid} ({shard_stats["role"]})'] = '{}/{}/{} GB'.format(
                    to_gb(min_ram_usage), to_gb(avg_total_ram_usage), to_gb(max_ram_usage))

        return [(not results[bdb['name']], kwargs[bdb['name']], f"check memory usage for '{bdb['name']}' (min/avg/max)")
                for bdb in bdbs]

