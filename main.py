import logging
import pprint

from checks import check_number_of_nodes, check_license, check_number_of_cores, check_shards_count, check_ram_size, \
    check_ephemeral_storage, check_persistent_storage, check_log_file_paths, check_tmp_file_path, check_quorum, \
    check_software_versions
from fetcher import fetch


def main():
    # pprint.pprint('CLUSTER')
    #pprint.pprint(fetch('cluster'))
    # pprint.pprint('NODES')
    #pprint.pprint(fetch('nodes'))
    # pprint.pprint('BDBS')
    #pprint.pprint(fetch('bdbs'))
    # pprint.pprint('SHARDS')
    # pprint.pprint(fetch('shards'))
    #
    # check_license()
    # check_shards_count(17)
    # check_number_of_nodes()
    # check_number_of_cores()
    # check_ram_size()
    # check_ephemeral_storage()
    # check_persistent_storage()
    #check_log_file_paths()
    #check_tmp_file_path()
    #check_software_versions()
    check_quorum()



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
