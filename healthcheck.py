#!/usr/bin/env python3

import argparse
import glob
import importlib
import logging
import pprint

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.check_executor import CheckExecutor


def load_suites(_args, _base_class=BaseCheckSuite):
    """
    Load check suites.

    :param _args: The pasred command line arguments.
    :param _base_class: The base class of the check suites.
    :return: A list with all instantiated check suites.
    """
    suites = []
    for file in glob.glob('healthcheck/check_suites/suite_*.py'):
        name = file.replace('/', '.').replace('.py', '')
        module = importlib.import_module(name)
        for member in dir(module):
            suite = getattr(module, member)
            if member != _base_class.__name__ and issubclass(suite.__class__, _base_class.__class__):
                suites.append(suite(_args))
    assert suites
    return suites


def parse_args():
    """
    Parse command line arguments.

    :return: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()

    cluster = parser.add_argument_group('cluster', 'credentials accessing the REST-API')
    cluster.add_argument('cluster_fqdn', help="The FQDN of the cluser to inspect.", type=str)
    cluster.add_argument('cluster_username', help="The username of the cluser to inspect.", type=str)
    cluster.add_argument('cluster_password', help="The password of the cluser to inspect.", type=str)

    ssh = parser.add_argument_group('ssh', 'credentials accessing the nodes via SSH')
    ssh.add_argument('ssh_username', help="The ssh username to log into nodes of the cluster.", type=str)
    ssh.add_argument('ssh_hostnames', help="A list with hostnames of the nodes.", type=str)
    ssh.add_argument('ssh_keyfile', help="The path to the ssh identity file.", type=str)

    return parser.parse_args()


def main(_args):

    # load check suites
    suites = load_suites(_args)

    # execute check suites
    executor = CheckExecutor(lambda x: pprint.pprint(x, width=160))
    for suite in suites:
        pprint.pprint('[SUITE] ' + suite.__doc__)
        executor.execute_suite(suite)

    # close
    executor.wait()
    executor.shutdown()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main(parse_args())
