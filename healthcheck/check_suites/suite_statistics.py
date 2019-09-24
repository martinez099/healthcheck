from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import GB


class StatChecks(BaseCheckSuite):
    """Check statistics"""

    def check_cluster(self):
        """check cluster statistics"""
        kwargs = {}
        stats = self.api.get('cluster/stats')

        ints = stats['intervals']

        # througput
        m = max([int['total_req'] for int in filter(lambda x: x.get('total_req'), ints)])
        kwargs['max_throughput'] = m

        # RAM usage
        m = min([int['free_memory'] for int in filter(lambda x: x.get('free_memory'), ints)])
        kwargs['max_memory_usage'] = m

        return None, kwargs

    def check_nodes(self):
        """check node statistics"""
        results = []
        kwargs = {}
        stats = self.api.get('nodes/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            kwargs[uid] = {}

            # RAM usage
            min_free_memory = min(int['free_memory'] for int in filter(lambda x: x.get('free_memory'), ints))
            total_memory = self.api.get(f'nodes/{uid}')['total_memory']
            result = min_free_memory < total_memory * (2/3)
            kwargs[uid]['low memory'] = result
            results.append(result)

            # CPU usage
            max_cpu_user = max(int['cpu_user'] for int in filter(lambda x: x.get('cpu_user'), ints))
            result = max_cpu_user > 0.8
            kwargs[uid]['high CPU'] = result
            results.append(result)

        return not any(results), kwargs

    def check_bdbs(self):
        """check bdb statistics"""
        results = []
        kwargs = {}
        stats = self.api.get('bdbs/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            name = self.api.get(f'bdbs/{uid}')['name']
            number_of_shards = self.api.get(f'bdbs/{uid}')['shards_count']
            bigstore = self.api.get(f'bdbs/{uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{uid}')['crdt_sync'] != 'disabled'
            kwargs[name] = {}

            # througput
            max_throughput = max([int['instantaneous_ops_per_sec'] for int in filter(lambda x: x.get('instantaneous_ops_per_sec'), ints)])
            if bigstore:
                result = max_throughput > (number_of_shards * 5000)
            elif crdb:
                result = max_throughput > (number_of_shards * 17500)
            else:
                result = max_throughput > (number_of_shards * 25000)
            kwargs[name]['high throughput'] = result
            results.append(result)

            # RAM usage
            max_memory_usage = max([int['used_memory'] for int in filter(lambda x: x.get('used_memory'), ints)])
            if bigstore:
                result = max_memory_usage > (number_of_shards * 50 * GB)
            else:
                result = max_memory_usage > (number_of_shards * 25 * GB)
            kwargs[name]['high memory'] = result
            results.append(result)

        return not any(results), kwargs

    def check_shards(self):
        """check shard statistcs"""
        results = []
        kwargs = {}
        stats = self.api.get('shards/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            bdb_uid = self.api.get(f'shards/{uid}')['bdb_uid']
            bigstore = self.api.get(f'bdbs/{bdb_uid}')['bigstore']
            crdb = self.api.get(f'bdbs/{bdb_uid}')['crdt_sync'] != 'disabled'
            kwargs[uid] = {}

            # througput
            max_total_requests = max([int['total_req'] for int in filter(lambda x: x.get('total_req'), ints)])
            if bigstore:
                result = max_total_requests > 5000
            elif crdb:
                result = max_total_requests > 17500
            else:
                result = max_total_requests > 25000
            kwargs[uid][' high throughput'] = result
            results.append(result)

            # RAM usage
            max_ram_usage = max([int['used_memory_peak'] for int in filter(lambda x: x.get('used_memory_peak'), ints)])
            if bigstore:
                result = max_ram_usage > (50 * GB)
            else:
                result = max_ram_usage > (25 * GB)
            kwargs[uid]['high memory'] = result
            results.append(result)

        return not any(results), kwargs
