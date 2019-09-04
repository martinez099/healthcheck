import datetime
import math

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_rex import SshRemoteExecutor

GB = pow(1024, 3)


def to_gb(_value):
    return math.floor(_value / GB)


def format_result(_result, **_kwargs):
    result = '[+] ' if _result else '[-] '
    return result + ', '.join([k + ': ' + str(v) for k, v in _kwargs.items()])


class CheckSuite(object):
    """
    Check Suite class.
    """

    def __init__(self, _args):
        """
        :param _args: The parsed command line arguments.
        """
        self.api = ApiFetcher(_args.cluster_fqdn, _args.cluster_username, _args.cluster_password)
        self.ssh = SshRemoteExecutor(_args.ssh_username, _args.ssh_hostnames.split(','), _args.ssh_keyfile)


class RecommendedRequirementsChecks(CheckSuite):
    """
    Recommended HW requirements checks.

    see https://docs.redislabs.com/latest/rs/administering/designing-production/hardware-requirements
    """

    def __init__(self, _args):
        """
        :param _args: The parsed command line arguments.
        """
        super(RecommendedRequirementsChecks, self).__init__(_args)

    def check_master_node(self, *_args, **_kwargs):
        master_node = self.ssh.get_master_node()

        return format_result(True, **{'master node': master_node})

    def check_software_version(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_nodes()
        software_versions = self.api.get_node_values('software_version')

        kwargs = {}
        for i in range(0, number_of_nodes):
            kwargs[f'software version (node{i + 1})'] = software_versions[i]

        return format_result(True, **kwargs)

    def check_license_shards_limit(self, *_args, **_kwargs):
        shards_limit = self.api.get_license_shards_limit()
        number_of_shards = self.api.get_number_of_shards()

        result = shards_limit >= number_of_shards
        return format_result(result, **{'shards limit': shards_limit,
                                        'number of shards': number_of_shards})

    def check_license_expire_date(self, *_args, **_kwargs):
        expire_date = self.api.get_license_expire_date()
        today = datetime.datetime.now()

        result = expire_date > today
        return format_result(result, **{'license expire date': expire_date,
                                        'today': today})

    def check_license_expired(self, *_args, **_kwargs):
        expired = self.api.get_license_expired()

        result = not expired
        return format_result(result, **{'license expired': expired})

    def check_number_of_shards(self, *_args, **_kwargs):
        number_of_shards = self.api.get_number_of_shards()
        min_shards = 2

        result = number_of_shards >= min_shards
        return format_result(result, **{'numbe of shards': number_of_shards,
                                        'min shards': min_shards})

    def check_number_of_nodes(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_nodes()
        min_nodes = 3

        result = number_of_nodes >= min_nodes
        return format_result(result, **{'number of nodes': number_of_nodes,
                                        'min nodes': min_nodes})

    def check_number_of_cores(self, *_args, **_kwargs):
        number_of_cores = self.api.get_sum_of_node_values('cores')
        min_cores = 24

        result = number_of_cores >= min_cores
        return format_result(result, **{'number of cores': number_of_cores,
                                        'min cores': min_cores})

    def check_total_memory(self, *_args, **_kwargs):
        total_memory = self.api.get_sum_of_node_values('total_memory')
        min_memory = 90 * GB

        result = total_memory >= min_memory
        return format_result(result, **{'total memory in GB': to_gb(total_memory),
                                        'min memory in GB': to_gb(min_memory)})

    def check_ephemeral_storage(self, *_args, **_kwargs):
        epehemeral_storage_size = self.api.get_sum_of_node_values('ephemeral_storage_size')
        min_ephemeral_size = 360 * GB

        result = epehemeral_storage_size >= min_ephemeral_size
        return format_result(result, **{'ephemeral storage size in GB': to_gb(epehemeral_storage_size),
                                        'min ephemeral size in GB': to_gb(min_ephemeral_size)})

    def check_persistent_storage(self, *_args, **_kwargs):
        persistent_storage_size = self.api.get_sum_of_node_values('persistent_storage_size')
        min_persistent_size = 540 * GB

        result = persistent_storage_size >= min_persistent_size
        return format_result(result, **{'persistent storage size in GB': to_gb(persistent_storage_size),
                                        'min persistent size in GB': to_gb(min_persistent_size)})

    def _check_quorum(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_nodes()
        quorums = [self.ssh.get_quorum(node_nr) for node_nr in range(0, number_of_nodes + 1)]
        return quorums

    def _check_log_file_paths(self, *_args, **_kwargs):
        log_file_path_node0 = self.ssh.get_log_file_path()

        result = '/dev/root' in log_file_path_node0
        return format_result(result, **{'log file path': log_file_path_node0})

    def _check_tmp_file_path(self, *_args, **_kwargs):
        tmp_file_paths = self.ssh.get_tmp_file_path()

        return tmp_file_paths

    def _check_memory_size(self, *_args, **_kwargs):
        memory_sizes = self.api.get_bdb_value(16, 'memory_size')

        return memory_sizes

    def _check_data_persistence(self, *_args, **_kwargs):
        data_persistences = self.api.get_bdb_value(16, 'data_persistence')

        return data_persistences

    def _check_rack_awareness(self, *_args, **_kwargs):
        rack_awareness = self.api.get_bdb_value(16, 'rack_aware')

        return rack_awareness

    def _check_reqplica_sync(self, *_args, **_kwargs):
        replica_sync = self.api.get_bdb_value(16, 'replica_sync')

        return replica_sync

    def _check_sync_sources(self, *_args, **_kwargs):
        sync_sources = self.api.get_bdb_value(16, 'sync_sources')

        return sync_sources

    def _check_shards_placement(self, *_args, **_kwargs):
        shards_placement = self.api.get_bdb_value(16, 'shards_placement')

        return shards_placement

    def _check_proxy_policy(self, *_args, **_kwargs):
        proxy_policy = self.api.get_bdb_value(16, 'proxy_policy')

        return proxy_policy

    def _check_replication(self, *_args, **_kwargs):
        replication = self.api.get_bdb_value(16, 'replication')

        return replication

    def _check_cluster_and_node_alerts(self, *_args, **_kwargs):
        alerts = self.api.get_cluster_value('alert_settings')

        return alerts

    def _check_bdb_alerts(self, *_args, **_kwargs):
        alerts = self.api.get_bdb_alerts()

        return alerts

    def _check_os_version(self, *_args, **_kwargs):
        os_version = self.api.get_node_values('os_version')

        return os_version

    def _check_swappiness(self, *_args, **_kwargs):
        swappiness = self.ssh.get_swappiness()

        return swappiness

    def _check_transparent_hugepages(self, *_args, **_kwargs):
        transparent_hugepages = self.ssh.get_transparent_hugepages()

        return transparent_hugepages

    def _check_rladmin_status(self, *_args, **_kwargs):
        status = self.ssh.run_rladmin_status()

        return status

    def _check_rlcheck_result(self, *_args, **_kwargs):
        check = self.ssh.run_rlcheck()

        return check

    def _check_cnm_ctl_status(self, *_args, **_kwargs):
        status = self.ssh.run_cnm_ctl_status()

        return status

    def _check_supervisorctl_status(self, *_args, **_kwargs):
        status = self.ssh.run_supervisorctl_status()

        return status

    def _check_errors_in_syslog(self, *_args, **_kwargs):
        errors = self.ssh.find_errors_in_syslog()

        return errors

    def _check_errors_in_install_log(self, *_args, **_kwargs):
        errors = self.ssh.find_errors_in_install_log()

        return errors

    def _check_errors_in_logs(self, *_args, **_kwargs):
        errors = self.ssh.find_errors_in_logs()

        return errors
