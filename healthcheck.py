#!/usr/bin/env python3

import argparse
import configparser
import glob
import importlib
import logging
import pprint

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.check_executor import CheckExecutor
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
            if member != _base_class.__name__:
                suite = getattr(module, member)
                if type(suite) == type.__class__ and issubclass(suite, _base_class):
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
        if result[1] is True:
            to_print = '[+] '
        elif result[1] is False:
            to_print = '[-] '
        elif result[1] is None:
            to_print = '[~] '
        elif result[1] is Exception:
            to_print = '[*] '
        else:
            pprint.pprint(f'[ ] [{result[0]}] skipped')
            return

        to_print += f'[{result[0]}] ' + f', '.join([k + ': ' + str(v) for k, v in result[2].items()])
        pprint.pprint(to_print, width=160)

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
            stats_collector.incr_succeeded()
        elif result[1] is False:
            stats_collector.incr_failed()
        elif result[1] is None:
            stats_collector.incr_no_result()
        elif result[1] is Exception:
            stats_collector.incr_errors()
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
