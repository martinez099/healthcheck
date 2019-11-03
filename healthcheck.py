#!/usr/bin/env python3

import argparse
import configparser
import json
import logging
import os

from healthcheck.check_suites.base_suite import load_suites
from healthcheck.check_executor import CheckExecutor
from healthcheck.stats_collector import StatsCollector
from healthcheck.render_engine import render_result, render_stats, render_list, print_error, print_msg
from healthcheck.common_funcs import get_parameter_map_name


def parse_args():
    """
    Parse command line arguments.

    :return: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()

    options = parser.add_argument_group()
    options.add_argument('-l', '--list', help="List all check suites.", action='store_true')
    options.add_argument('-s', '--suite', help="Specify a suite to execute.", type=str)
    options.add_argument('-c', '--check', help="Specify a check to execute.", type=str, default='all')
    options.add_argument('-p', '--params', help="Specify a parameter map to use.", type=str)
    options.add_argument('-cfg', '--config', help="Path to config file", type=str, default='config.ini')

    return parser.parse_args()


def parse_config(args):
    """
    Parse configuration file.

    :return: The parsed configuration.
    """
    if not os.path.isfile(args.config):
        print_error('could not find configuration file, examine argument of --config')
        exit(1)

    config = configparser.ConfigParser()
    with open(args.config, 'r') as configfile:
        config.read_file(configfile)
    return config


def load_parameter_map(suite, args):
    """
    Load parameter map.

    :param suite: The check suite.
    :param args: The parsed arguments.
    :return: A list of tuples.
    """
    if not args.params:
        if suite.params:
            print_error('missing parameter map, pass argument to --params')
            exit(1)

        return None

    if args.params.endswith('.json'):
        if not os.path.exists(args.params):
            print_error("could not find parameter map, examine argument of --params")
            exit(1)

        with open(args.params) as file:
            params = [(args.params, json.loads(file.read()))]

    else:
        params = list(filter(lambda x: args.params.lower() in get_parameter_map_name(x[0].lower()),
                             suite.params.items()))
        if args.params and not params:
            print_error('could not find paramter map, options are: {}'.format(
                list(map(get_parameter_map_name, suite.params.keys()))))
            exit(1)

        if len(params) > 1:
            print_error('multiple parameter maps found, options are: {}'.format(
                list(map(get_parameter_map_name, suite.params.keys()))))
            exit(1)

    return params


def exec_singele_check(suites, args, executor):
    """
    Execute single checks.

    :param suites: The loaded check suites.
    :param args: The parsed arguments.
    :param executor: The check executor.
    """
    checks = []
    for suite in suites:
        for check in filter(lambda x: x.startswith('check_'), dir(suite)):
            check_func = getattr(suite, check)
            if args.check.lower() in check_func.__doc__.lower():
                checks.append((check_func, suite))

    if not checks:
        print_error('could not find any single check, examine argument of --check')
        exit(1)

    for check, suite in checks:
        params = load_parameter_map(suite, args)
        executor.execute(check, _kwargs=params[0][1] if params else {})

    executor.wait()
    executor.shutdown()


def exec_check_suite(suites, args, executor):
    """
    Execute a check suite.

    :param suites: The loaded check suites.
    :param args: The parsed arguments.
    :param executor: The check executor.
    """
    stats_collector = StatsCollector()

    def collect(_future):
        result = _future.result()
        [stats_collector.collect(r) for r in result] if type(result) == list else stats_collector.collect(result)

    if len(suites) != 1:
        if args.suite:
            print_error('could not find check suite, examine argument of --suite')
        else:
            print_error('missing check suite, pass argument to --suite')
        exit(1)

    to_print = [f'running check suite: {suites[0].__doc__}']
    params = load_parameter_map(suites[0], args)
    if params:
        to_print.append('- using paramter map: {}'.format(get_parameter_map_name(params[0][0])))

    print_msg('\n'.join(to_print))
    suites[0].run_connectivity_checks()
    executor.execute_suite(suites[0], _kwargs=params[0][1] if params else {}, _done_cb=collect)
    executor.wait()

    render_stats(stats_collector)


def main():

    # configure logging
    logging.basicConfig(level=logging.INFO)

    # parse command line arguments
    args = parse_args()

    # parse configuration file
    config = parse_config(args)

    # load check suites
    suites = load_suites(args, config)

    # list suites
    if args.list:
        render_list(suites)
        return

    # render result
    def render(_result, _func, _args, _kwargs):
        if type(_result) == list:
            return [render(r, _func, _args, _kwargs) for r in _result]
        else:
            return render_result(_result, _func)

    # execute checks
    executor = CheckExecutor(render)
    if args.check != 'all':
        exec_singele_check(suites, args, executor)
    else:
        exec_check_suite(suites, args, executor)

    # shutdown
    executor.shutdown()
    logging.shutdown()


if __name__ == '__main__':
    main()
