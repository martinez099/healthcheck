from checks import get_cluster, get_nodes, get_bdbs, get_shards, check_number_of_nodes, check_license, \
    check_number_of_cores, check_shards_count


def main():
    get_cluster()
    get_nodes()
    get_bdbs()
    get_shards()

    check_license()
    check_shards_count(17)
    check_number_of_nodes()
    check_number_of_cores()


if __name__ == '__main__':
    main()
