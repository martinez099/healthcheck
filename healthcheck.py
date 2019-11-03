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
    options.add_argument('-p', '--paramap', help="Specify a parameter map to use.", type=str)
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

    # create executor
    executor = CheckExecutor(render)

    # execute single checks
    if args.check != 'all':
        checks = []
        for suite in suites:
            for check in filter(lambda x: x.startswith('check_'), dir(suite)):
                check_func = getattr(suite, check)
                if args.check.lower() in check_func.__doc__.lower():
                    checks.append(check_func)

        if not checks:
            print_error('could not find any single check, examine argument of --check')
            exit(1)

        for check in checks:
            executor.execute(check)

        executor.wait()
        executor.shutdown()
        return

    # init statistic collector
    stats_collector = StatsCollector()

    # collect statistics
    def collect(_future):
        result = _future.result()
        [stats_collector.collect(r) for r in result] if type(result) == list else stats_collector.collect(result)

    # execute check suites
    if len(suites) != 1:
        if args.suite:
            print_error('could not find check suite, examine argument of --suite')
        else:
            print_error('missing check suite, pass argument to --suite')
        exit(1)

    if suites[0].params and not args.paramap:
        print_error('missing parameter map, pass argument to --paramap')
        exit(1)

    params = None
    to_print = [f'running check suite: {suites[0].__doc__}']
    if args.paramap:
        if args.paramap.endswith('.json'):
            if not os.path.exists(args.paramap):
                print_error("could not find parameter map, examine argument of --paramap")
                exit(1)

            with open(args.paramap) as file:
                params = [(args.paramap, json.loads(file.read()))]

        else:
            params = list(filter(lambda x: args.paramap.lower() in get_parameter_map_name(x[0].lower()), suites[0].params.items()))
            if args.paramap and not params:
                print_error('could not find paramter map, options are: {}'.format(list(map(get_parameter_map_name, suites[0].params.keys()))))
                exit(1)

            if len(params) > 1:
                print_error('multiple parameter maps found, options are: {}'.format(list(map(get_parameter_map_name, suites[0].params.keys()))))
                exit(1)

        to_print.append('- using paramter map: {}'.format(get_parameter_map_name(params[0][0])))

    print_msg('\n'.join(to_print))
    suites[0].run_connectivity_checks()
    executor.execute_suite(suites[0], _kwargs=params[0][1] if params else {}, _done_cb=collect)
    executor.wait()

    # print statistics
    render_stats(stats_collector)

    # close
    executor.shutdown()
    logging.shutdown()


if __name__ == '__main__':
    main()
