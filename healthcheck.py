#!/usr/bin/env python3

import argparse
import configparser
import glob
import importlib
import logging
import pprint

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.check_executor import CheckExecutor
from healthcheck.common import format_result
from healthcheck.stats_collector import StatsCollector


def load_suites(_args, _config, _base_class=BaseCheckSuite):
    """
    Load check suites.

    :param _args: The pasred command line arguments.
    :param _config: The configuration.
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
                    suites.append(suite(_config))
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
    options.add_argument('-s', '--suite', help="Specify a single suite to execute.", type=str, default='all')
    options.add_argument('-c', '--check', help="Specify a single check to execute.", type=str, default='all')

    return parser.parse_args()


def parse_config():
    """
    Parse configuration file.

    :return: The parsed configuration.
    """
    config = configparser.ConfigParser()
    with open('config.ini', 'r') as configfile:
        config.read_file(configfile)
    return config


def main():

    # configure logging
    logging.basicConfig(level=logging.INFO)

    # parse command line arguments
    args = parse_args()

    # parse configuration file
    config = parse_config()

    # load check suites
    suites = load_suites(args, config)

    # list suites
    if args.list:
        for suite in suites:
            pprint.pprint(f'{suite.__class__.__name__}: {suite.__doc__}')
        return

    # render output
    def result_cb(result):
        pprint.pprint(format_result(result[0], result[1], **result[2]), width=160)

    # create check executor
    executor = CheckExecutor(result_cb)

    # execute single checks
    if args.check != 'all':
        for suite in suites:
            checks_names = filter(lambda x: x.startswith('check_') and args.check.lower() in x.lower(), dir(suite))
            for check_name in checks_names:
                check_func = getattr(suite, check_name)
                executor.execute(check_func)
        executor.wait()
        executor.shutdown()
        return

    # init statistic collector
    stats_collector = StatsCollector()

    # collect statistics
    def done_cb(future):
        result = future.result()
        if result[1] is True:
            stats_collector.incr_success()
        elif result[1] is False:
            stats_collector.incr_failed()
        elif result[1] is None:
            stats_collector.incr_no_result()
        elif result[1] is Exception:
            stats_collector.incr_error()
        else:
            stats_collector.incr_skipped()

    # execute all check suites
    for suite in suites:
        pprint.pprint('[SUITE] ' + suite.__doc__)
        executor.execute_suite(suite, done_cb)
        executor.wait()

    # print statistics
    pprint.pprint("[COLLECTED STATISTICS]")
    pprint.pprint(stats_collector.get_stats())

    # close
    executor.shutdown()


if __name__ == '__main__':
    main()
