from checker import *


def main():

    # check_license()
    # check_shards_count()
    # check_number_of_nodes()
    # check_number_of_cores()
    # check_ram_size()
    # check_ephemeral_storage()
    # check_persistent_storage()
    # check_log_file_paths()
    # check_tmp_file_path()
    # check_software_version()
    # check_quorum()
    # check_master_node()
    # check_memory_size()
    # check_data_persistence()
    # check_rack_awareness()
    # check_reqplica_sync()
    # check_sync_sources()
    # check_shards_placement()
    # check_proxy_policy()
    # check_replication()
    # check_cluster_and_node_alerts()
    # check_bdb_alerts()
    # check_os_version()
    # check_swappiness()
    # check_transparent_hugepages()
    # check_rladmin_status()
    # check_rlcheck_result()
    # check_cnm_ctl_status()
    # check_supervisorctl_status()
    # check_errors_in_syslog()
    # check_errors_in_install_log()
    check_errors_in_logs()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
