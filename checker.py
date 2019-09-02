import pprint

from api_fetcher import *
from ssh_rex import *


def check_license():
    shards_limit = get_shards_limit()
    pprint.pprint('shards_limit: {}'.format(shards_limit))


def check_shards_count():
    shards_count = get_shards_count()
    pprint.pprint('shards_count: {}'.format(shards_count))


def check_number_of_nodes():
    number_of_nodes = get_number_of_nodes()
    pprint.pprint('number_of_nodes: {}'.format(number_of_nodes))


def check_number_of_cores():
    number_of_cores = get_summed_nodes_values('cores')
    pprint.pprint('number_of_cores: {}'.format(number_of_cores))


def check_ram_size():
    total_memory = get_summed_nodes_values('total_memory')
    pprint.pprint('total_memory: {}'.format(total_memory))


def check_ephemeral_storage():
    epehemeral_storage = get_nodes_values('ephemeral_storage_size')
    pprint.pprint('ephemeral_storage_size: {}'.format(epehemeral_storage), width=160)


def check_persistent_storage():
    persistent_storage = get_nodes_values('persistent_storage_size')
    pprint.pprint('persistent_storage_size: {}'.format(persistent_storage), width=160)


def check_log_file_paths():
    log_file_paths = get_log_file_paths()
    pprint.pprint('log_file_path: {}'.format(log_file_paths))


def check_tmp_file_path():
    tmp_file_paths = get_tmp_file_path()
    pprint.pprint('tmp_file_paths: {}'.format(tmp_file_paths))


def check_software_version():
    software_versions = get_nodes_values('software_version')
    pprint.pprint('softwar_versions: {}'.format(software_versions))


def check_quorum():
    number_of_nodes = get_number_of_nodes()
    quorums = get_quorums(number_of_nodes)
    pprint.pprint(quorums)


def check_master_node():
    master_node = get_master_node()
    pprint.pprint(master_node, width=180)


def check_memory_size():
    memory_sizes = get_bdb_value(16, 'memory_size')
    pprint.pprint(memory_sizes)


def check_data_persistence():
    data_persistences = get_bdb_value(16, 'data_persistence')
    pprint.pprint(data_persistences)


def check_rack_awareness():
    rack_awareness = get_bdb_value(16, 'rack_aware')
    pprint.pprint(rack_awareness)


def check_reqplica_sync():
    replica_sync = get_bdb_value(16, 'replica_sync')
    pprint.pprint(replica_sync)


def check_sync_sources():
    sync_sources = get_bdb_value(16, 'sync_sources')
    pprint.pprint(sync_sources)


def check_shards_placement():
    shards_placement = get_bdb_value(16, 'shards_placement')
    pprint.pprint(shards_placement)


def check_proxy_policy():
    proxy_policy = get_bdb_value(16, 'proxy_policy')
    pprint.pprint(proxy_policy)


def check_replication():
    replication = get_bdb_value(16, 'replication')
    pprint.pprint(replication)


def check_cluster_and_node_alerts():
    alerts = get_cluster_value('alert_settings')
    pprint.pprint(alerts)


def check_bdb_alerts():
    alerts = get_bdb_alerts()
    pprint.pprint(alerts)


def check_os_version():
    os_version = get_nodes_values('os_version')
    pprint.pprint(os_version)


def check_swappiness():
    swappiness = get_swappiness()
    pprint.pprint(swappiness)


def check_transparent_hugepages():
    transparent_hugepages = get_transparent_hugepages()
    pprint.pprint(transparent_hugepages)


def check_rladmin_status():
    status = run_rladmin_status()
    pprint.pprint(status)


def check_rlcheck_result():
    check = run_rlcheck()
    pprint.pprint(check)


def check_cnm_ctl_status():
    status = run_cnm_ctl_status()
    pprint.pprint(status)


def check_supervisorctl_status():
    status = run_supervisorctl_status()
    pprint.pprint(status)


def check_errors_in_syslog():
    errors = find_errors_in_syslog()
    pprint.pprint(errors)


def check_errors_in_install_log():
    errors = find_errors_in_install_log()
    pprint.pprint(errors)


def check_errors_in_logs():
    errors = find_errors_in_logs()
    pprint.pprint(errors)
