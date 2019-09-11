import datetime
import re

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import to_gb, GB


class ClusterChecks(BaseCheckSuite):
    """Check Cluster via API"""

    def check_license_shards_limit(self, *_args, **_kwargs):
        """"check if shards limit in license is respected"""
        number_of_shards = self.api.get_number_of_values('shards')
        match = re.search(r'Shards limit : (\d+)\n', self.api.get('license')['license'], re.MULTILINE | re.DOTALL)
        shards_limit = int(match.group(1))

        result = shards_limit >= number_of_shards
        kwargs = {'shards limit': shards_limit, 'number of shards': number_of_shards}
        return result, kwargs

    def check_license_expire_date(self, *_args, **_kwargs):
        """check if expire date is in future"""
        expire_date = datetime.datetime.strptime(self.api.get('license')['expiration_date'], '%Y-%m-%dT%H:%M:%SZ')
        today = datetime.datetime.now()

        result = expire_date > today
        kwargs = {'license expire date': expire_date, 'today': today}
        return result, kwargs

    def check_license_expired(self, *_args, **_kwargs):
        """check if license is expired"""
        expired = self.api.get('license')['expired']

        return not expired, {'license expired': expired}

    def check_number_of_shards(self, *_args, **_kwargs):
        """check if enough shards"""
        number_of_shards = self.api.get_number_of_values('shards')

        result = number_of_shards >= _kwargs['min_shards']
        kwargs = {'numbe of shards': number_of_shards, 'min shards': _kwargs['min_shards']}
        return result, kwargs

    def check_number_of_nodes(self, *_args, **_kwargs):
        """check if enough nodes"""
        number_of_nodes = self.api.get_number_of_values('nodes')

        result = number_of_nodes >= _kwargs['min_nodes']
        kwargs = {'number of nodes': number_of_nodes, 'min nodes': _kwargs['min_nodes']}
        return result, kwargs

    def check_number_of_cores(self, *_args, **_kwargs):
        """check if enough cores"""
        number_of_cores = self.api.get_sum_of_values('nodes', 'cores')

        result = number_of_cores >= _kwargs['min_cores']
        kwargs = {'number of cores': number_of_cores, 'min cores': _kwargs['min_cores']}
        return result, kwargs

    def check_total_memory(self, *_args, **_kwargs):
        """check if enough RAM"""
        total_memory = self.api.get_sum_of_values('nodes', 'total_memory')

        result = total_memory >= _kwargs['min_memory'] * GB
        kwargs = {'total memory': '{} GB'.format(to_gb(total_memory)), 'min memory': '{} GB'.format(_kwargs['min_memory'])}
        return result, kwargs

    def check_ephemeral_storage(self, *_args, **_kwargs):
        """check if enough ephemeral storage"""
        epehemeral_storage_size = self.api.get_sum_of_values('nodes', 'ephemeral_storage_size')

        result = epehemeral_storage_size >= _kwargs['min_ephemeral_storage'] * GB
        kwargs = {'ephemeral storage size': '{} GB'.format(to_gb(epehemeral_storage_size)),
                  'min ephemeral size': '{} GB'.format(_kwargs['min_ephemeral_storage'])}
        return result, kwargs

    def check_persistent_storage(self, *_args, **_kwargs):
        """check if enough persistent storage"""
        persistent_storage_size = self.api.get_sum_of_values('nodes', 'persistent_storage_size')

        result = persistent_storage_size >= _kwargs['min_persistent_storage'] * GB
        kwargs = {'persistent storage size': '{} GB'.format(to_gb(persistent_storage_size)),
                  'min persistent size': '{} GB'.format(_kwargs['min_persistent_storage'] )}
        return result, kwargs

    def check_cluster_and_node_alert_settings(self, *_args, **_kwargs):
        """get cluster and node alert settings"""
        alerts = self.api.get_value('cluster', 'alert_settings')

        kwargs = {'alerts': alerts}
        return None, kwargs
