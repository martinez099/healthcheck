#!/usr/bin/env python3

import argparse
import logging

from health_checker import HealthChecker


def healtcheck(_args):

    checker = HealthChecker(_args)

    checker.check_license()
    checker.check_shards_count()
    checker.check_number_of_nodes()
    checker.check_number_of_cores()
    checker.check_ram_size()
    checker.check_ephemeral_storage()
    checker.check_persistent_storage()
    checker.check_log_file_paths()
    checker.check_tmp_file_path()
    checker.check_software_version()
    checker.check_quorum()
    checker.check_master_node()
    checker.check_memory_size()
    checker.check_data_persistence()
    checker.check_rack_awareness()
    checker.check_reqplica_sync()
    checker.check_sync_sources()
    checker.check_shards_placement()
    checker.check_proxy_policy()
    checker.check_replication()
    checker.check_cluster_and_node_alerts()
    checker.check_bdb_alerts()
    checker.check_os_version()
    checker.check_swappiness()
    checker.check_transparent_hugepages()
    checker.check_rladmin_status()
    checker.check_rlcheck_result()
    checker.check_cnm_ctl_status()
    checker.check_supervisorctl_status()
    checker.check_errors_in_syslog()
    checker.check_errors_in_install_log()
    #checker.check_errors_in_logs()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('cluster_fqdn', help="The FQDN of the cluser to inspect.", type=str)
    parser.add_argument('cluster_username', help="The username of the cluser to inspect.", type=str)
    parser.add_argument('cluster_password', help="The password of the cluser to inspect.", type=str)
    parser.add_argument('ssh_username', help="The ssh username to log into nodes of the cluster.", type=str)
    parser.add_argument('ssh_hostnames', help="A list with hostnames of the nodes of the cluster.", type=str)
    parser.add_argument('ssh_keyfile', help="The path to the ssh identity file.", type=str)
    args = parser.parse_args()

    healtcheck(args)
