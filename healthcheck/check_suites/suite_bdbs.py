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

        return all(kwargs.values()) if kwargs.values() else '', kwargs

    def check_bdbs(self, *_args, **_kwargs):
        """check database configuration"""
        bdbs = self.api.get('bdbs')
        results = []
        for bdb in bdbs:
            values = _kwargs['__default__']
            if bdb['name'] in _kwargs:
                values.update(_kwargs[bdb['name']])
            result = self._check_bdb(bdb['uid'], values)
            results.append(result)

        return results

    def _check_bdb(self, _uid, _values):
        bdb = self.api.get(f'bdbs/{_uid}')
        kwargs = {}
        for k, v in _values.items():
            result = v == bdb[k]
            if not result:
                kwargs[k] = bdb[k]

        return not bool(kwargs), kwargs, f"""check configuration of '{bdb['name']}'"""

    def check_cpu_usage(self, *_args, **_kwargs):
        """check for too high throughput"""
        kwargs = {}
        stats = self.api.get('shards/stats')

        for stat_idx in range(0, len(stats)):
            uid = stats[stat_idx]['uid']
            bdb_uid = self.api.get(f'shards/{uid}')['bdb_uid']
            bdb_name = self.api.get(f'bdbs/{bdb_uid}')['name']
            bigstore = self.api.get(f'bdbs/{bdb_uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{bdb_uid}')['crdt_sync'] != 'disabled'

            max_total_requests = max([i['total_req'] for i in filter(lambda x: x.get('total_req'), stats[stat_idx]['intervals'])])
            if bigstore:
                result = max_total_requests > 5000
            elif crdb:
                result = max_total_requests > 17500
            else:
                result = max_total_requests > 25000
            if result:
                if bdb_name not in kwargs:
                    kwargs[bdb_name] = {}
                kwargs[bdb_name][f'shard:{uid}'] = '{}K ops/sec'.format(to_kops(max_total_requests))

        return not any(any(result.values()) for result in kwargs.values()), kwargs

    def check_ram_usage(self, *_args, **_kwargs):
        """check for too much memory usage"""
        kwargs = {}
        stats = self.api.get('shards/stats')

        for stat_idx in range(0, len(stats)):
            uid = stats[stat_idx]['uid']
            bdb_uid = self.api.get(f'shards/{uid}')['bdb_uid']
            bdb_name = self.api.get(f'bdbs/{bdb_uid}')['name']
            bigstore = self.api.get(f'bdbs/{bdb_uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{bdb_uid}')['crdt_sync'] != 'disabled'

            max_ram_usage = max([i['used_memory'] for i in filter(lambda x: x.get('used_memory'), stats[stat_idx]['intervals'])])
            if bigstore:
                result = max_ram_usage > (50 * GB)
            else:
                result = max_ram_usage > (25 * GB)
            if result:
                if bdb_name not in kwargs:
                    kwargs[bdb_name] = {}
                kwargs[bdb_name][f'shard:{uid}'] = '{} GB'.format(to_gb(max_ram_usage))

        return not any(any(result.values()) for result in kwargs.values()), kwargs

    def check_cpu_balance(self, *_args, **_kwargs):
        """get CPU balance"""
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

            results.append((None, kwargs, f'get CPU balance of \'{bdb["name"]}\''))

        return results

    def check_ram_balance(self, *_args, **_kwargs):
        """get RAM balance"""
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

            results.append((None, kwargs, f'get RAM balance of \'{bdb["name"]}\''))

        return results
