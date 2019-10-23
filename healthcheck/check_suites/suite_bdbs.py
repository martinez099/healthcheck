from healthcheck.check_suites.base_suite import BaseCheckSuite, load_params
from healthcheck.common_funcs import GB


class BdbChecks(BaseCheckSuite):
    """Databases"""

    def __init__(self, _config):
        super().__init__(_config)
        self.params = load_params('params_bdbs')

    def _check_connectivity(self):
        self._check_api_connectivity()

    def check_oss_api(self, *_args, **_kwargs):
        """check for OSS cluster API"""
        bdbs = self.api.get('bdbs')
        kwargs = {}
        for bdb in filter(lambda x: x['oss_cluster'], bdbs):
            kwargs[bdb['name']] = bdb['shards_placement'] == 'sparse' and bdb['proxy_policy'] == 'all-master-shards'

        return all(kwargs.values()), kwargs

    def _check_bdb_alert_settings(self, *_args, **_kwargs):
        """get database alert settings"""
        alerts = self.api.get('bdbs/alerts')

        return None, {'alerts': alerts}

    def check_bdbs(self, *_args, **_kwargs):
        """check databases according to given paramter map"""
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
        f"""check bdb:{_uid}"""
        bdb = self.api.get(f'bdbs/{_uid}')
        kwargs = {bdb['name']: {}}
        for k, v in _values.items():
            result = v == bdb[k]
            if not result:
                kwargs[bdb['name']][k] = bdb[k]

        return not any(kwargs[bdb['name']].values()), kwargs

    def check_stats(self, *_args, **_kwargs):
        """check bdb statistics"""
        kwargs = {}
        stats = self.api.get('bdbs/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            name = self.api.get(f'bdbs/{uid}')['name']
            number_of_shards = self.api.get(f'bdbs/{uid}')['shards_count']
            # TODO: take care of replication
            bigstore = self.api.get(f'bdbs/{uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{uid}')['crdt_sync'] != 'disabled'
            kwargs[name] = {}

            # througput
            max_throughput = max([i['instantaneous_ops_per_sec'] for i in filter(lambda x: x.get('instantaneous_ops_per_sec'), ints)])
            if bigstore:
                result = max_throughput > (number_of_shards * 5000)
            elif crdb:
                result = max_throughput > (number_of_shards * 17500)
            else:
                result = max_throughput > (number_of_shards * 25000)
            if result:
                kwargs[name]['too much throughput'] = result

            # RAM usage
            max_memory_usage = max([i['used_memory'] for i in filter(lambda x: x.get('used_memory'), ints)])
            if bigstore:
                result = max_memory_usage > (number_of_shards * 50 * GB)
            else:
                result = max_memory_usage > (number_of_shards * 25 * GB)
            if result:
                kwargs[name]['too much memory usage'] = result

        return not any(any(result.values()) for result in kwargs.values()), kwargs

    def check_shard_stats(self, *_args, **_kwargs):
        """check shard statistcs"""
        kwargs = {}
        stats = self.api.get('shards/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            bdb_uid = self.api.get(f'shards/{uid}')['bdb_uid']
            bigstore = self.api.get(f'bdbs/{bdb_uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{bdb_uid}')['crdt_sync'] != 'disabled'
            kwargs[f'shard:{uid}'] = {}

            # througput
            max_total_requests = max([i['total_req'] for i in filter(lambda x: x.get('total_req'), ints)])
            if bigstore:
                result = max_total_requests > 5000
            elif crdb:
                result = max_total_requests > 17500
            else:
                result = max_total_requests > 25000
            if result:
                kwargs[f'shard:{uid}']['too much throughput'] = result

            # RAM usage
            max_ram_usage = max([i['used_memory_peak'] for i in filter(lambda x: x.get('used_memory_peak'), ints)])
            if bigstore:
                result = max_ram_usage > (50 * GB)
            else:
                result = max_ram_usage > (25 * GB)
            if result:
                kwargs[f'shard:{uid}']['too much memory usage'] = result

        return not any(any(result.values()) for result in kwargs.values()), kwargs
