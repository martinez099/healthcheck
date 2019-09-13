from healthcheck.check_suites.base_suite import BaseCheckSuite


class StatChecks(BaseCheckSuite):
    """Check statistics via API"""

    def check_cluster(self):
        """check cluster statistics"""
        kwargs = {}
        stats = self.api.get('cluster/stats')

        ints = stats['intervals']
        uid = stats['uid']

        # througput
        m = max([int['total_req'] for int in filter(lambda x: x.get('total_req'), ints)])
        kwargs['max_throughput'] = m

        # RAM usage
        m = min([int['free_memory'] for int in filter(lambda x: x.get('free_memory'), ints)])
        kwargs['max_memory_usage'] = m

        return None, kwargs

    def check_nodes(self):
        """check node statistics"""
        kwargs = {}
        stats = self.api.get('nodes/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            kwargs[uid] = {}

            # througput
            m = max([int['total_req'] for int in filter(lambda x: x.get('total_req'), ints)])
            kwargs[uid]['max_throughput'] = m

            # RAM usage
            m = min([int['free_memory'] for int in filter(lambda x: x.get('free_memory'), ints)])
            kwargs[uid]['min_free_memory'] = m

        return None, kwargs

    def check_bdbs(self):
        """check bdb statistics"""
        kwargs = {}
        stats = self.api.get('bdbs/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            kwargs[uid] = {}

            # througput
            m = max([int['instantaneous_ops_per_sec'] for int in filter(lambda x: x.get('instantaneous_ops_per_sec'), ints)])
            kwargs[uid]['max_throughput'] = m

            # RAM usage
            m = max([int['used_memory'] for int in filter(lambda x: x.get('used_memory'), ints)])
            kwargs[uid]['max_memory_usage'] = m

        return None, kwargs

    def check_shards(self):
        """check shard statistcs"""

        kwargs = {}
        stats = self.api.get('shards/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']
            kwargs[uid] = {}

            # througput
            m = max([int['total_req'] for int in filter(lambda x: x.get('total_req'), ints)])
            kwargs[uid]['max_throughput'] = m

            # RAM usage
            m = max([int['used_memory_peak'] for int in filter(lambda x: x.get('used_memory_peak'), ints)])
            kwargs[uid]['max_memory_usage'] = m

        return None, kwargs
