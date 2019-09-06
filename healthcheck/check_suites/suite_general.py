import datetime

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import format_result


class GeneralChecks(BaseCheckSuite):
    """Check General Settings"""

    def check_master_node(self, *_args, **_kwargs):
        """get hostname of master node"""
        master_node = self.ssh.get_master_node()

        return format_result(True, **{'master node': master_node})

    def check_software_version(self, *_args, **_kwargs):
        """get software version of all nodes"""
        number_of_nodes = self.api.get_number_of_nodes()
        software_versions = self.api.get_node_values('software_version')

        kwargs = {f'node{i + 1}': software_versions[i] for i in range(0, number_of_nodes)}
        return format_result(None, **kwargs)

    def check_os_version(self, *_args, **_kwargs):
        """get os version of all nodes"""
        number_of_nodes = self.api.get_number_of_nodes()
        os_versions = self.api.get_node_values('os_version')

        kwargs = {f'node{i + 1}': os_versions[i] for i in range(0, number_of_nodes)}
        return format_result(None, **kwargs)

    def check_license_shards_limit(self, *_args, **_kwargs):
        """check if shards limit in license is respected"""
        shards_limit = self.api.get_license_shards_limit()
        number_of_shards = self.api.get_number_of_shards()

        result = shards_limit >= number_of_shards
        return format_result(result, **{'shards limit': shards_limit,
                                        'number of shards': number_of_shards})

    def check_license_expire_date(self, *_args, **_kwargs):
        """check if expire date is in future"""
        expire_date = self.api.get_license_expire_date()
        today = datetime.datetime.now()

        result = expire_date > today
        return format_result(result, **{'license expire date': expire_date,
                                        'today': today})

    def check_license_expired(self, *_args, **_kwargs):
        """check if license is expired"""
        expired = self.api.get_license_expired()

        result = not expired
        return format_result(result, **{'license expired': expired})

    def check_quorum_only(self, *_args, **_kwargs):
        """get quorumg only nodes"""
        number_of_nodes = self.api.get_number_of_nodes()
        quorums = self.ssh.get_quorum_onlys(number_of_nodes)

        kwargs = {f'node{i + 1}': quorums[i] for i in range(0, number_of_nodes)}
        return format_result(None, **kwargs)

    def check_log_file_path(self, *_args, **_kwargs):
        """check if log file path is on root filesystem"""
        number_of_nodes = self.api.get_number_of_nodes()
        log_file_paths = self.ssh.get_log_file_paths(number_of_nodes)

        result = any(['/dev/root' not in log_file_path for log_file_path in log_file_paths])
        kwargs = {f'node{i + 1}': log_file_paths[i] for i in range(0, number_of_nodes)}
        return format_result(result, **kwargs)

    def check_tmp_file_path(self, *_args, **_kwargs):
        """check if tmp file path is on root filesystem"""
        number_of_nodes = self.api.get_number_of_nodes()
        tmp_file_paths = self.ssh.get_tmp_file_paths(number_of_nodes)

        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in tmp_file_paths])
        kwargs = {f'node{i + 1}': tmp_file_paths[i] for i in range(0, number_of_nodes)}
        return format_result(result, **kwargs)

    def check_swappiness(self, *_args, **_kwargs):
        """get swap setting of all nodes"""
        number_of_nodes = self.api.get_number_of_nodes()
        swappinesses = self.ssh.get_swappinesses(number_of_nodes)

        kwargs = {f'node{i + 1}': swappinesses[i] for i in range(0, number_of_nodes)}
        return format_result(None, **kwargs)

    def check_transparent_hugepage(self, *_args, **_kwargs):
        """get THP setting of all nodes"""
        number_of_nodes = self.api.get_number_of_nodes()
        transparent_hugepages = self.ssh.get_transaprent_hugepages(number_of_nodes)

        kwargs = {f'node{i + 1}': transparent_hugepages[i] for i in range(0, number_of_nodes)}
        return format_result(None, **kwargs)
