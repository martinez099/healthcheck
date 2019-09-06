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
                if _args.suite == 'all' or _args.suite.lower() in member.lower():
                    suites.append(suite(_args))
    assert suites
    return suites


def parse_args():
    """
    Parse command line arguments.

    :return: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()

    options = parser.add_mutually_exclusive_group()
    options.add_argument('-l', '--list', help="List all check suites.", action='store_true')
    options.add_argument('-s', '--suite', help="Specify a check suite to execute.", type=str, default='all')

    cluster = parser.add_argument_group('cluster', 'credentials accessing the REST-API')
    cluster.add_argument('cluster_fqdn', help="The FQDN of the Redis Enterprise cluser.", type=str)
    cluster.add_argument('cluster_username', help="The basic auth username of the cluser.", type=str)
    cluster.add_argument('cluster_password', help="The basic auth password of the cluser.", type=str)

    ssh = parser.add_argument_group('ssh', 'credentials accessing the nodes via SSH')
    ssh.add_argument('ssh_username', help="The SSH username to log into nodes of the cluster.", type=str)
    ssh.add_argument('ssh_hostnames', help="A list with hostnames of the nodes.", type=str)
    ssh.add_argument('ssh_keyfile', help="The path to the SSH identity file.", type=str)

    return parser.parse_args()


def main(_args):

    # load check suites
    suites = load_suites(_args)

    if _args.list:
        for suite in suites:
            pprint.pprint(f'{suite.__class__.__name__}: {suite.__doc__}')
        return

    # execute check suites
    executor = CheckExecutor(lambda x: pprint.pprint(x, width=160))
    for suite in suites:
        pprint.pprint('[SUITE] ' + suite.__doc__)
        executor.execute_suite(suite)
        executor.wait()

    # close
    executor.shutdown()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main(parse_args())
