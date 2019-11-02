import re

from healthcheck.check_suites.base_suite import BaseCheckSuite, load_params
from healthcheck.common_funcs import GB, to_gb, to_kops


class ClusterChecks(BaseCheckSuite):
    """Cluster"""

    def __init__(self, _config):
        super().__init__(_config)
        self.params = load_params('cluster')

    def _check_connectivity(self):
        self._check_api_connectivity()

    def check_license_shards_limit(self, *_args, **_kwargs):
        """check if shards limit in license"""
        number_of_shards = self.api.get_number_of_values('shards')
        match = re.search(r'Shards limit : (\d+)\n', self.api.get('license')['license'], re.MULTILINE | re.DOTALL)
        shards_limit = int(match.group(1))

        result = shards_limit >= number_of_shards
        kwargs = {'shards limit': shards_limit, 'number of shards': number_of_shards}
        return result, kwargs

    def check_license_expired(self, *_args, **_kwargs):
        """check if license is expired"""
        expired = self.api.get('license')['expired']

        return not expired, {'license expired': expired}

    def check_number_of_shards(self, *_args, **_kwargs):
        """check if enough shards according to HW requirements"""
        number_of_shards = self.api.get_number_of_values('shards')

        result = number_of_shards >= _kwargs['min_shards']
        kwargs = {'number of shards': number_of_shards, 'min shards': _kwargs['min_shards']}
        return result, kwargs

    def check_number_of_nodes(self, *_args, **_kwargs):
        """check if enough nodes according to HW requirements"""
        number_of_nodes = self.api.get_number_of_values('nodes')

        result = number_of_nodes >= _kwargs['min_nodes'] and number_of_nodes % 2 != 0
        kwargs = {'number of nodes': number_of_nodes, 'min nodes': _kwargs['min_nodes']}
        return result, kwargs

    def check_number_of_cores(self, *_args, **_kwargs):
        """check if enough cores according to HW requirements"""
        number_of_cores = self.api.get_sum_of_values('nodes', 'cores')

        result = number_of_cores >= _kwargs['min_cores']
        kwargs = {'number of cores': number_of_cores, 'min cores': _kwargs['min_cores']}
        return result, kwargs

    def check_total_memory(self, *_args, **_kwargs):
        """check if enough RAM according to HW requirements"""
        total_memory = self.api.get_sum_of_values('nodes', 'total_memory')

        result = total_memory >= _kwargs['min_memory'] * GB
        kwargs = {'total memory': '{} GB'.format(to_gb(total_memory)), 'min memory': '{} GB'.format(_kwargs['min_memory'])}
        return result, kwargs

    def check_ephemeral_storage(self, *_args, **_kwargs):
        """check if enough ephemeral storage according to HW requirements"""
        epehemeral_storage_size = self.api.get_sum_of_values('nodes', 'ephemeral_storage_size')

        result = epehemeral_storage_size >= _kwargs['min_ephemeral_storage'] * GB
        kwargs = {'ephemeral storage size': '{} GB'.format(to_gb(epehemeral_storage_size)),
                  'min ephemeral size': '{} GB'.format(_kwargs['min_ephemeral_storage'])}
        return result, kwargs

    def check_persistent_storage(self, *_args, **_kwargs):
        """check if enough persistent storage according to HW requirements"""
        persistent_storage_size = self.api.get_sum_of_values('nodes', 'persistent_storage_size')

        result = persistent_storage_size >= _kwargs['min_persistent_storage'] * GB
        kwargs = {'persistent storage size': '{} GB'.format(to_gb(persistent_storage_size)),
                  'min persistent size': '{} GB'.format(_kwargs['min_persistent_storage'] )}
        return result, kwargs

    def _check_alert_settings(self, *_args, **_kwargs):
        """get cluster and node alert settings"""
        alerts = self.api.get_value('cluster', 'alert_settings')

        kwargs = {'alerts': alerts}
        return None, kwargs

    def check_cpu_usage(self, *_args, **_kwargs):
        """get CPU usage"""
        kwargs = {}
        stats = self.api.get('cluster/stats')

        m = max([i['total_req'] for i in filter(lambda x: x.get('total_req'), stats['intervals'])])
        kwargs['maximum throughput'] = '{}K ops/sec'.format(to_kops(m))

        return None, kwargs

    def check_ram_usage(self, *_args, **_kwargs):
        """get RAM usage"""
        kwargs = {}
        stats = self.api.get('cluster/stats')

        m = min([i['available_memory'] for i in filter(lambda x: x.get('available_memory'), stats['intervals'])])
        kwargs['minimum available memory'] = '{} GB'.format(to_gb(m))

        return None, kwargs

    def check_storage_usage(self, *_args, **_kwargs):
        """get storage usage"""
        kwargs = {}
        stats = self.api.get('cluster/stats')

        # persistent storage
        m = min([i['persistent_storage_avail'] for i in filter(lambda x: x.get('persistent_storage_avail'), stats['intervals'])])
        kwargs['minimum available peristent storage'] = '{} GB'.format(to_gb(m))

        # ephemeral storage
        m = min([i['ephemeral_storage_avail'] for i in filter(lambda x: x.get('ephemeral_storage_avail'), stats['intervals'])])
        kwargs['minimum available ephemeral storage'] = '{} GB'.format(to_gb(m))

        return None, kwargs
