#!/usr/bin/env python3

import argparse
import logging
import pprint

from healtcheck.check_executor import CheckExecutor
from healtcheck.health_checks import HealthChecks


def main(_args):

    checks = HealthChecks(_args)
    executor = CheckExecutor(lambda x: pprint.pprint(x, width=160))

    executor.execute(checks.check_master_node)
    executor.execute(checks.check_software_version)
    executor.execute(checks.check_license_shards_limit)
    executor.execute(checks.check_license_expire_date)
    executor.execute(checks.check_license_expired)
    executor.execute(checks.check_number_of_shards)
    executor.execute(checks.check_number_of_nodes)
    executor.execute(checks.check_number_of_cores)
    executor.execute(checks.check_total_memory)
    executor.execute(checks.check_ephemeral_storage)
    executor.execute(checks.check_persistent_storage)
    executor.execute(checks.check_quorum)
    executor.execute(checks.check_log_file_paths)
    executor.execute(checks.check_tmp_file_path)
    executor.execute(checks.check_memory_size)
    executor.execute(checks.check_data_persistence)
    executor.execute(checks.check_rack_awareness)
    executor.execute(checks.check_reqplica_sync)
    executor.execute(checks.check_sync_sources)
    executor.execute(checks.check_shards_placement)
    executor.execute(checks.check_proxy_policy)
    executor.execute(checks.check_replication)
    executor.execute(checks.check_cluster_and_node_alerts)
    executor.execute(checks.check_bdb_alerts)
    executor.execute(checks.check_os_version)
    executor.execute(checks.check_swappiness)
    executor.execute(checks.check_transparent_hugepages)
    executor.execute(checks.check_rladmin_status)
    executor.execute(checks.check_rlcheck_result)
    executor.execute(checks.check_cnm_ctl_status)
    executor.execute(checks.check_supervisorctl_status)
    executor.execute(checks.check_errors_in_syslog)
    executor.execute(checks.check_errors_in_install_log)
    #executore.execute(check.check_errors_in_logs)

    executor.wait()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    cluster = parser.add_argument_group('cluster', 'data accessing the Redis E cluster')
    cluster.add_argument('cluster_fqdn', help="The FQDN of the cluser to inspect.", type=str)
    cluster.add_argument('cluster_username', help="The username of the cluser to inspect.", type=str)
    cluster.add_argument('cluster_password', help="The password of the cluser to inspect.", type=str)
    ssh = parser.add_argument_group('ssh', 'data accessing the nodes vie SSH')
    ssh.add_argument('ssh_username', help="The ssh username to log into nodes of the cluster.", type=str)
    ssh.add_argument('ssh_hostnames', help="A list with hostnames of the nodes.", type=str)
    ssh.add_argument('ssh_keyfile', help="The path to the ssh identity file.", type=str)
    args = parser.parse_args()

    main(args)
