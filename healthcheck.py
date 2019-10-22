#!/usr/bin/env python3

import argparse
import configparser
import logging
import os

from healthcheck.check_suites.base_suite import load_suites
from healthcheck.check_executor import CheckExecutor
from healthcheck.stats_collector import StatsCollector
from healthcheck.render_engine import render_result, render_stats, render_list, print_error
from healthcheck.common_funcs import get_parameter_map_name


def parse_args():
    """
    Parse command line arguments.

    :return: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()

    options = parser.add_argument_group()
    options.add_argument('-l', '--list', help="List all check suites.", action='store_true')
    options.add_argument('-s', '--suite', help="Specify a suite to execute.", type=str, default='nodes')
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
        print_error('Could not find configuration file, examine argument of --config!')
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
        found = False
        for suite in suites:
            for check in filter(lambda x: x.startswith('check_'), dir(suite)):
                check_func = getattr(suite, check)
                if args.check.lower() in check_func.__doc__.lower():
                    found = True
                    print('Running single check: {} ...'.format(check_func.__doc__))
                    suite.connectivity_check()
                    executor.execute(check_func)

        if not found:
            print_error('Could not find any single check, examine argument of --check!')
            exit(1)

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
    if not suites:
        print_error('Could not find any check suite, examine argument of --suite!')
        exit(1)

    for suite in suites:
        if suite.params and not args.paramap:
            print_error('Missing parameter map, pass argument to --paramap!')
            exit(1)

        to_print = [f'Running check suite: {suite.__doc__} ...']
        params = None
        if args.paramap:
            params = list(filter(lambda x: args.paramap.lower() in get_parameter_map_name(x[0].lower()), suite.params.items()))
            if args.paramap and not params:
                print_error('Could not find paramter map, pass argument to --list!')
                exit(1)

            if len(params) > 1:
                print_error('Multiple parameter maps found, examine argument of --paramap!')
                exit(1)

            to_print.append('- using paramter map: {}'.format(get_parameter_map_name(params[0][0])))

        print('\n'.join(to_print))
        suite.connectivity_check()
        executor.execute_suite(suite, _kwargs=params[0][1] if params else {}, _done_cb=collect)
        executor.wait()

    # print statistics
    render_stats(stats_collector)

    # close
    executor.shutdown()
    logging.shutdown()


if __name__ == '__main__':
    main()
