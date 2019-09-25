import datetime
import re

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import to_gb, GB


class ClusterChecks(BaseCheckSuite):
    """Check cluster [params]"""

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

        result = number_of_nodes >= _kwargs['min_nodes'] and number_of_nodes % 2 != 0
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

    def check_alert_settings(self, *_args, **_kwargs):
        """get cluster and node alert settings"""
        alerts = self.api.get_value('cluster', 'alert_settings')

        kwargs = {'alerts': alerts}
        return None, kwargs

    def check_shards_balance(self, *_args, **_kwargs):
        """check if shards are balanced accross nodes"""
        nodes = self.api.get('nodes')
        node_ids = list(map(lambda x: x['uid'], nodes))
        rsps = [self.ssh.exec_on_host(f'sudo /opt/redislabs/bin/rladmin info node {uid}',
                                      self.ssh.hostnames[0]) for uid in node_ids]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = {node_id: match.group(1) == 'enabled' for node_id, match in zip(node_ids, matches)}
        shards = self.api.get('shards')
        shards_per_node = {}
        for shard in shards:
            if shard['node_uid'] not in shards_per_node:
                shards_per_node[shard['node_uid']] = 0
            shards_per_node[shard['node_uid']] += 1

        result = True
        for node_id, shards in shards_per_node.items():
            if quorum_onlys[int(node_id)] and shards > 0:
                result = False

        unbalanced = max(shards_per_node.values()) - min(shards_per_node.values()) > 1
        return result and not unbalanced, shards_per_node
