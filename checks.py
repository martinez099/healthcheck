import pprint

from fetcher import fetch_shards_limit, fetch_shards_count, fetch_number_of_nodes, fetch_number_of_cores, \
    fetch_ram_size, fetch_ephemeral_storage, fetch_persistent_storage, fetch_log_file_paths, fetch_tmp_file_path, \
    fetch_software_versions, fetch_quorums


def check_license():
    shards_limit = fetch_shards_limit()
    pprint.pprint('shards_limit: {}'.format(shards_limit))


def check_shards_count():
    shards_count = fetch_shards_count()
    pprint.pprint('shards_count: {}'.format(shards_count))


def check_number_of_nodes():
    number_of_nodes = fetch_number_of_nodes()
    pprint.pprint('number_of_nodes: {}'.format(number_of_nodes))


def check_number_of_cores():
    number_of_cores = fetch_number_of_cores()
    pprint.pprint('number_of_cores: {}'.format(number_of_cores))


def check_ram_size():
    number_of_cores = fetch_ram_size()
    pprint.pprint('total_memory: {}'.format(number_of_cores))


def check_ephemeral_storage():
    number_of_cores = fetch_ephemeral_storage()
    pprint.pprint('ephemeral_storage_size: {}'.format(number_of_cores))


def check_persistent_storage():
    number_of_cores = fetch_persistent_storage()
    pprint.pprint('persistent_storage_size: {}'.format(number_of_cores))


def check_log_file_paths():
    log_file_paths = fetch_log_file_paths()
    pprint.pprint(log_file_paths)


def check_tmp_file_path():
    tmp_file_paths = fetch_tmp_file_path()
    pprint.pprint(tmp_file_paths)


def check_software_versions():
    software_versions = fetch_software_versions()
    pprint.pprint(software_versions)


def check_quorum():
    quroums = fetch_quorums()
    pprint.pprint(quroums)
