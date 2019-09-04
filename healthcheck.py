#!/usr/bin/env python3

import argparse
import logging
import pprint

from healthcheck.check_executor import CheckExecutor
from healthcheck.health_checks import RecommendedRequirementsChecks


def main(_args):

    # execute recommended HW requirements check suite
    executor = CheckExecutor(lambda x: pprint.pprint(x, width=160))
    executor.execute_suite(RecommendedRequirementsChecks(_args))
    executor.wait()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # parse command line arguments
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
