#!/usr/bin/env python3

import argparse
import configparser
import logging
import os

from healthcheck.check_suites.base_suite import load_suites
from healthcheck.check_executor import CheckExecutor
from healthcheck.stats_collector import StatsCollector
from healthcheck.ssh_commander import SshCommander
from healthcheck.api_fetcher import ApiFetcher
from healthcheck.render_engine import render_result, render_stats, render_list, print_error, print_success


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
    options.add_argument('-p', '--params', help="Specify a parameter map to use.", type=str)
    options.add_argument('-cfg', '--config', help="Path to config file", type=str, default='config.ini')

    return parser.parse_args()


def parse_config(args):
    """
    Parse configuration file.

    :return: The parsed configuration.
    """
    if not os.path.isfile(args.config):
        raise Exception('could not find configfuration file')

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

    # check SSH connectivity
    print('checking SSH connectivity ...')
    ssh = SshCommander(config['ssh']['hosts'], config['ssh']['user'], config['ssh']['key'])
    for ip in ssh.hostnames:
        try:
            ssh.exec_on_host('sudo -v', ip)
            print_success(f'successfully connected to {ip}')
        except Exception as e:
            print_error(f'could not connect to host {ip}')
            raise e

    # check API connectivity
    try:
        print('checking API connectivity ...')
        api = ApiFetcher(config['api']['fqdn'], config['api']['user'], config['api']['pass'])
        fqdn = api.get_value('cluster', 'name')
        print_success(f'successfully connected to {fqdn}')
    except Exception as e:
        print_error('could not connect to Redis Enterprise REST-API')
        raise e

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
                    print('\nRunning single check: {} ...'.format(check_func.__doc__))
                    executor.execute(check_func)

        if not found:
            print_error('\nCould not find any single check, check -c!')
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
        print_error('\nCould not find any check suite, check -s!')
        exit(1)

    for suite in suites:
        if suite.params and not args.params:
            print_error('\nMissing parameter map, use -p!')
            exit(1)

        print(f'\nRunning check suite: {suite.__doc__} ...')
        params = None
        if args.params:
            params = list(filter(lambda x: args.params.lower() in x[0].lower(), suite.params.items()))
            if args.params and not params:
                print_error('Could not find paramter map, use -l!')
                exit(1)

            if len(params) > 1:
                print_error('Multiple parameter maps found, check -p!')
                exit(1)

            print('- using paramter map: {}'.format(params[0][0]))

        executor.execute_suite(suite, _kwargs=params[0][1] if params else {}, _done_cb=collect)
        executor.wait()

    # print statistics
    render_stats(stats_collector)

    # close
    executor.shutdown()
    logging.shutdown()


if __name__ == '__main__':
    main()
