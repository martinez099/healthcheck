import datetime

from healtcheck.api_fetcher import ApiFetcher
from healtcheck.ssh_rex import SshRemoteExecutor


class HealthChecks(object):
    """
    Health Checks class.
    """

    def __init__(self, _args):
        """
        :param _args: The parsed command line arguments.
        """
        self.api = ApiFetcher(_args.cluster_fqdn, _args.cluster_username, _args.cluster_password)
        self.ssh = SshRemoteExecutor(_args.ssh_username, _args.ssh_hostnames.split(','), _args.ssh_keyfile)

    def check_master_node(self, *_args, **_kwargs):
        master_node = self.ssh.get_master_node()

        return HealthChecks._format(True, **{'master node': master_node})

    def check_software_version(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_nodes()
        software_versions = self.api.get_node_values('software_version')

        kwargs = {}

        for i in range(0, number_of_nodes):
            kwargs[f'node{i + 1}'] = {
                'software version': software_versions[i]
            }

        #return HealthChecks._format_list(software_versions,**kwargs)
        return software_versions

    def check_license_shards_limit(self, *_args, **_kwargs):
        shards_limit = self.api.get_license_shards_limit()
        number_of_shards = self.api.get_number_of_shards()

        result = shards_limit >= number_of_shards
        return HealthChecks._format(result, **{'shards limit': shards_limit,
                                               'number of shards': number_of_shards})

    def check_license_expire_date(self, *_args, **_kwargs):
        expire_date = self.api.get_license_expire_date()
        today = datetime.datetime.now()

        result = expire_date > today
        return HealthChecks._format(result, **{'license expire date': expire_date,
                                               'today': today})

    def check_license_expired(self, *_args, **_kwargs):
        expired = self.api.get_license_expired()

        return HealthChecks._format(not expired, **{'license expired': expired})

    def check_number_of_shards(self, *_args, **_kwargs):
        number_of_shards = self.api.get_number_of_shards()
        min_shards = 2

        result = number_of_shards >= min_shards
        return HealthChecks._format(result, **{'numbe of shards': number_of_shards,
                                               'min shards': min_shards})

    def check_number_of_nodes(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_nodes()
        min_nodes = 3

        result = number_of_nodes >= min_nodes
        return HealthChecks._format(result, **{'number of nodes': number_of_nodes,
                                               'min nodes': min_nodes})

    def check_number_of_cores(self, *_args, **_kwargs):
        number_of_cores = self.api.get_sum_of_node_values('cores')
        min_cores = 24

        result = number_of_cores >= min_cores
        return HealthChecks._format(result, **{'number of cores': number_of_cores,
                                               'min cores': min_cores})

    def check_total_memory(self, *_args, **_kwargs):
        total_memory = self.api.get_sum_of_node_values('total_memory')
        min_memory = 90 * 10e7  # GB

        result = total_memory >= min_memory
        return HealthChecks._format(result, **{'total memory': total_memory,
                                               'min memory': min_memory})

    def check_ephemeral_storage(self, *_args, **_kwargs):
        epehemeral_storage_size = self.api.get_sum_of_node_values('ephemeral_storage_size')
        min_ephemeral_size = 360 * 10e7  # GB

        result = epehemeral_storage_size >= min_ephemeral_size
        return HealthChecks._format(result, **{'ephemeral storage size': epehemeral_storage_size,
                                               'min ephemeral size': min_ephemeral_size})

    def check_persistent_storage(self, *_args, **_kwargs):
        persistent_storage_size = self.api.get_sum_of_node_values('persistent_storage_size')
        min_persistent_size = 540 * 10e7  # GB

        result = persistent_storage_size >= min_persistent_size
        return HealthChecks._format(result, **{'persistent storage size': persistent_storage_size,
                                               'min persistent size': min_persistent_size})

    def check_quorum(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_nodes()
        quorums = [self.ssh.get_quorum(node_nr) for node_nr in range(0, number_of_nodes + 1)]
        return quorums

    def check_log_file_paths(self, *_args, **_kwargs):
        log_file_path_node0 = self.ssh.get_log_file_path()

        result = '/dev/root' in log_file_path_node0
        return HealthChecks._format(result, **{'log file path': log_file_path_node0})

    def check_tmp_file_path(self, *_args, **_kwargs):
        tmp_file_paths = self.ssh.get_tmp_file_path()

        return tmp_file_paths

    def check_memory_size(self, *_args, **_kwargs):
        memory_sizes = self.api.get_bdb_value(16, 'memory_size')

        return memory_sizes

    def check_data_persistence(self, *_args, **_kwargs):
        data_persistences = self.api.get_bdb_value(16, 'data_persistence')

        return data_persistences

    def check_rack_awareness(self, *_args, **_kwargs):
        rack_awareness = self.api.get_bdb_value(16, 'rack_aware')

        return rack_awareness

    def check_reqplica_sync(self, *_args, **_kwargs):
        replica_sync = self.api.get_bdb_value(16, 'replica_sync')

        return replica_sync

    def check_sync_sources(self, *_args, **_kwargs):
        sync_sources = self.api.get_bdb_value(16, 'sync_sources')

        return sync_sources

    def check_shards_placement(self, *_args, **_kwargs):
        shards_placement = self.api.get_bdb_value(16, 'shards_placement')

        return shards_placement

    def check_proxy_policy(self, *_args, **_kwargs):
        proxy_policy = self.api.get_bdb_value(16, 'proxy_policy')

        return proxy_policy

    def check_replication(self, *_args, **_kwargs):
        replication = self.api.get_bdb_value(16, 'replication')

        return replication

    def check_cluster_and_node_alerts(self, *_args, **_kwargs):
        alerts = self.api.get_cluster_value('alert_settings')

        return alerts

    def check_bdb_alerts(self, *_args, **_kwargs):
        alerts = self.api.get_bdb_alerts()

        return alerts

    def check_os_version(self, *_args, **_kwargs):
        os_version = self.api.get_node_values('os_version')

        return os_version

    def check_swappiness(self, *_args, **_kwargs):
        swappiness = self.ssh.get_swappiness()

        return swappiness

    def check_transparent_hugepages(self, *_args, **_kwargs):
        transparent_hugepages = self.ssh.get_transparent_hugepages()

        return transparent_hugepages

    def check_rladmin_status(self, *_args, **_kwargs):
        status = self.ssh.run_rladmin_status()

        return status

    def check_rlcheck_result(self, *_args, **_kwargs):
        check = self.ssh.run_rlcheck()

        return check

    def check_cnm_ctl_status(self, *_args, **_kwargs):
        status = self.ssh.run_cnm_ctl_status()

        return status

    def check_supervisorctl_status(self, *_args, **_kwargs):
        status = self.ssh.run_supervisorctl_status()

        return status

    def check_errors_in_syslog(self, *_args, **_kwargs):
        errors = self.ssh.find_errors_in_syslog()

        return errors

    def check_errors_in_install_log(self, *_args, **_kwargs):
        errors = self.ssh.find_errors_in_install_log()

        return errors

    def check_errors_in_logs(self, *_args, **_kwargs):
        errors = self.ssh.find_errors_in_logs()

        return errors

    @staticmethod
    def _format(_result, **_kwargs):
        result = '+ [POSITIVE] ' if _result else '- [NEGATIVE] '
        return result + ', '.join([k + ': ' + str(v) for k, v in _kwargs.items()])

    @staticmethod
    def _format_list(_results, **_kwargs):
        results = ['']
        for i in range(0, len(_results)):
            results.append(k + ': ' + HealthChecks._format(_results[i], **_kwargs))
        return ''.join(results)
