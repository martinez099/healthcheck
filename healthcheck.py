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
from healthcheck.render_engine import render_result, render_stats, render_list


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
    logging.info(f'checking SSH connectivity ...')
    ssh = SshCommander(config['ssh']['user'], config['ssh']['hosts'], config['ssh']['key'])
    for ip in ssh.hostnames:
        try:
            ssh.exec_on_host('sudo -v', ip)
            logging.info(f'successfully connected to {ip}')
        except Exception as e:
            logging.error(f'could not connect to host {ip}')
            raise e

    # check API connectivity
    try:
        logging.info('checking API connectivity ...')
        api = ApiFetcher(config['api']['fqdn'], config['api']['user'], config['api']['pass'])
        fqdn = api.get_value('cluster', 'name')
        logging.info('successfully connected to {}'.format(fqdn))
    except Exception as e:
        logging.error('could not connect to Redis Enterprise REST-API', e)
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
        for suite in suites:
            check_names = filter(lambda x: x.startswith('check_') and args.check.lower() in x.lower(), dir(suite))
            for check_name in check_names:
                print('Running single check [{}] ...'.format(check_name))
                check_func = getattr(suite, check_name)
                executor.execute(check_func)
        executor.wait()
        executor.shutdown()
        return

    # init statistic collector
    stats_collector = StatsCollector()

    # collect statistics
    def collect_stats(_future):
        result = _future.result()
        [stats_collector.collect(r) for r in result] if type(result) == list else stats_collector.collect(result)

    # execute check suites
    for suite in suites:
        to_print = 'Running check suite [{}] ...'.format(suite.__doc__)
        if args.params:
            to_print += ' ' + args.params
        print(to_print)
        params = map(lambda x: x[1], filter(lambda x: args.params in x[0].lower(), suite.params.items()))
        executor.execute_suite(suite, _kwargs=list(params)[0] if args.params else {}, _done_cb=collect_stats)
        executor.wait()

    # print statistics
    render_stats(stats_collector)

    # close
    executor.shutdown()
    logging.shutdown()


if __name__ == '__main__':
    main()
