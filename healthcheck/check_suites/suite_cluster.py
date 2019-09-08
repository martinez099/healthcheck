import datetime
import re

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import to_gb, GB


class ClusterChecks(BaseCheckSuite):
    """Check Cluster Configuration"""

    def check_master_node(self, *_args, **_kwargs):
        rsp = self.ssh.run_rladmin_status()
        found = re.search(r'(node:\d+ master.*)', rsp)
        hostname = re.split(r'\s+', found.group(1))[4]
        ip_address = re.split(r'\s+', found.group(1))[3]

        return "get master node", True, {'hostname': hostname, 'IP address': ip_address}

    def check_software_version(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_values('nodes')
        software_versions = self.api.get_values('nodes', 'software_version')

        kwargs = {f'node{i + 1}': software_versions[i] for i in range(0, number_of_nodes)}
        return "get software version of all nodes", None, kwargs

    def check_license_shards_limit(self, *_args, **_kwargs):
        number_of_shards = self.api.get_number_of_values('shards')
        match = re.search(r'Shards limit : (\d+)\n', self.api.get('license')['license'], re.MULTILINE | re.DOTALL)
        shards_limit = int(match.group(1))

        result = shards_limit >= number_of_shards
        kwargs = {'shards limit': shards_limit, 'number of shards': number_of_shards}
        return "check if shards limit in license is respected", result, kwargs

    def check_license_expire_date(self, *_args, **_kwargs):
        expire_date = datetime.datetime.strptime(self.api.get('license')['expiration_date'], '%Y-%m-%dT%H:%M:%SZ')
        today = datetime.datetime.now()

        result = expire_date > today
        kwargs = {'license expire date': expire_date, 'today': today}
        return "check if expire date is in future", result, kwargs

    def check_license_expired(self, *_args, **_kwargs):
        expired = self.api.get('license')['expired']

        return "check if license is expired", not expired, {'license expired': expired}

    def check_number_of_shards(self, *_args, **_kwargs):
        number_of_shards = self.api.get_number_of_values('shards')
        min_shards = 2

        result = number_of_shards >= min_shards
        kwargs = {'numbe of shards': number_of_shards, 'min shards': min_shards}
        return "check if enough shards", result, kwargs

    def check_number_of_nodes(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_values('nodes')
        min_nodes = 3

        result = number_of_nodes >= min_nodes
        kwargs = {'number of nodes': number_of_nodes, 'min nodes': min_nodes}
        return "check if enough nodes",result, kwargs

    def check_number_of_cores(self, *_args, **_kwargs):
        number_of_cores = self.api.get_sum_of_values('nodes', 'cores')
        min_cores = 24

        result = number_of_cores >= min_cores
        kwargs = {'number of cores': number_of_cores, 'min cores': min_cores}
        return "check if enough cores", result, kwargs

    def check_total_memory(self, *_args, **_kwargs):
        total_memory = self.api.get_sum_of_values('nodes', 'total_memory')
        min_memory = 90 * GB

        result = total_memory >= min_memory
        kwargs = {'total memory': to_gb(total_memory), 'min memory': to_gb(min_memory)}
        return "check if enough RAM", result, kwargs

    def check_ephemeral_storage(self, *_args, **_kwargs):
        epehemeral_storage_size = self.api.get_sum_of_values('nodes', 'ephemeral_storage_size')
        min_ephemeral_size = 360 * GB

        result = epehemeral_storage_size >= min_ephemeral_size
        kwargs = {'ephemeral storage size': to_gb(epehemeral_storage_size),
                  'min ephemeral size': to_gb(min_ephemeral_size)}
        return "check if enough ephemeral storage", result, kwargs

    def check_persistent_storage(self, *_args, **_kwargs):
        persistent_storage_size = self.api.get_sum_of_values('nodes', 'persistent_storage_size')
        min_persistent_size = 540 * GB

        result = persistent_storage_size >= min_persistent_size
        kwargs =  {'persistent storage size': to_gb(persistent_storage_size),
                   'min persistent size': to_gb(min_persistent_size)}
        return "check if enough persistent storage", result, kwargs

    def check_cluster_and_node_alerts(self, *_args, **_kwargs):
        alerts = self.api.get_value('cluster', 'alert_settings')

        return "check cluster and node alerts", None, {'alerts': alerts}

    def check_cluster_and_node_alert_settings(self, *_args, **_kwargs):
        alerts = self.api.get_value('cluster', 'alert_settings')

        kwargs = {'alerts': alerts}
        return "get cluster and node alert settings", None, kwargs

    def check_quorum_only(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_values('nodes')
        rsp = self.ssh.get_quorum_onlys(number_of_nodes)
        match = re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL)
        quorums = match.group(1)

        kwargs = {f'node{i + 1}': quorums[i] for i in range(0, number_of_nodes)}
        return "get quorumg only nodes", None, kwargs
