import pprint

from api_fetcher import ApiFetcher
from ssh_rex import SshRemoteExecutor


class HealthChecker(object):
    """
    Health Checker class.
    """

    def __init__(self, _args):
        """
        :param _args: The parsed command line arguments.
        """
        self.api = ApiFetcher(_args.cluster_fqdn, _args.cluster_username, _args.cluster_password)
        self.ssh = SshRemoteExecutor(_args.ssh_username, _args.ssh_hostnames.split(','), _args.ssh_keyfile)

    def check_license(self):
        shards_limit = self.api.get_shards_limit()
        pprint.pprint('shards_limit: {}'.format(shards_limit))

    def check_shards_count(self):
        shards_count = self.api.get_shards_count()
        pprint.pprint('shards_count: {}'.format(shards_count))

    def check_number_of_nodes(self):
        number_of_nodes = self.api.get_number_of_nodes()
        pprint.pprint('number_of_nodes: {}'.format(number_of_nodes))

    def check_number_of_cores(self):
        number_of_cores = self.api.get_summed_node_values('cores')
        pprint.pprint('number_of_cores: {}'.format(number_of_cores))

    def check_ram_size(self):
        total_memory = self.api.get_summed_node_values('total_memory')
        pprint.pprint('total_memory: {}'.format(total_memory))

    def check_ephemeral_storage(self):
        epehemeral_storage = self.api.get_node_values('ephemeral_storage_size')
        pprint.pprint('ephemeral_storage_size: {}'.format(epehemeral_storage), width=160)

    def check_persistent_storage(self):
        persistent_storage = self.api.get_node_values('persistent_storage_size')
        pprint.pprint('persistent_storage_size: {}'.format(persistent_storage), width=160)

    def check_log_file_paths(self):
        log_file_paths = self.ssh.get_log_file_paths()
        pprint.pprint('log_file_path: {}'.format(log_file_paths))

    def check_tmp_file_path(self):
        tmp_file_paths = self.ssh.get_tmp_file_path()
        pprint.pprint('tmp_file_paths: {}'.format(tmp_file_paths))

    def check_software_version(self):
        software_versions = self.api.get_node_values('software_version')
        pprint.pprint('softwar_versions: {}'.format(software_versions))

    def check_quorum(self):
        number_of_nodes = self.api.get_number_of_nodes()
        quorums = self.ssh.get_quorums(number_of_nodes)
        pprint.pprint(quorums)

    def check_master_node(self):
        master_node = self.ssh.get_master_node()
        pprint.pprint(master_node, width=180)

    def check_memory_size(self):
        memory_sizes = self.api.get_bdb_value(16, 'memory_size')
        pprint.pprint(memory_sizes)

    def check_data_persistence(self):
        data_persistences = self.api.get_bdb_value(16, 'data_persistence')
        pprint.pprint(data_persistences)

    def check_rack_awareness(self):
        rack_awareness = self.api.get_bdb_value(16, 'rack_aware')
        pprint.pprint(rack_awareness)

    def check_reqplica_sync(self):
        replica_sync = self.api.get_bdb_value(16, 'replica_sync')
        pprint.pprint(replica_sync)

    def check_sync_sources(self):
        sync_sources = self.api.get_bdb_value(16, 'sync_sources')
        pprint.pprint(sync_sources)

    def check_shards_placement(self):
        shards_placement = self.api.get_bdb_value(16, 'shards_placement')
        pprint.pprint(shards_placement)

    def check_proxy_policy(self):
        proxy_policy = self.api.get_bdb_value(16, 'proxy_policy')
        pprint.pprint(proxy_policy)

    def check_replication(self):
        replication = self.api.get_bdb_value(16, 'replication')
        pprint.pprint(replication)

    def check_cluster_and_node_alerts(self):
        alerts = self.api.get_cluster_value('alert_settings')
        pprint.pprint(alerts)

    def check_bdb_alerts(self):
        alerts = self.api.get_bdb_alerts()
        pprint.pprint(alerts)

    def check_os_version(self):
        os_version = self.api.get_node_values('os_version')
        pprint.pprint(os_version)

    def check_swappiness(self):
        swappiness = self.ssh.get_swappiness()
        pprint.pprint(swappiness)

    def check_transparent_hugepages(self):
        transparent_hugepages = self.ssh.get_transparent_hugepages()
        pprint.pprint(transparent_hugepages)

    def check_rladmin_status(self):
        status = self.ssh.run_rladmin_status()
        pprint.pprint(status)

    def check_rlcheck_result(self):
        check = self.ssh.run_rlcheck()
        pprint.pprint(check)

    def check_cnm_ctl_status(self):
        status = self.ssh.run_cnm_ctl_status()
        pprint.pprint(status)

    def check_supervisorctl_status(self):
        status = self.ssh.run_supervisorctl_status()
        pprint.pprint(status)

    def check_errors_in_syslog(self):
        errors = self.ssh.find_errors_in_syslog()
        pprint.pprint(errors)

    def check_errors_in_install_log(self):
        errors = self.ssh.find_errors_in_install_log()
        pprint.pprint(errors)

    def check_errors_in_logs(self):
        errors = self.ssh.find_errors_in_logs()
        pprint.pprint(errors)
