#!/usr/bin/env python3

import argparse
import configparser
import logging
import pprint
import os

from healthcheck.check_suites.base_suite import load_suites
from healthcheck.check_executor import CheckExecutor
from healthcheck.stats_collector import StatsCollector
from healthcheck.ssh_commander import SshCommander
from healthcheck.api_fetcher import ApiFetcher


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
        result = []
        for suite in suites:
            result.append(f'{suite.__class__.__name__}: {suite.__doc__}')
            check_names = filter(lambda x: x.startswith('check_'), dir(suite))
            checks = []
            for check_name in check_names:
                check_func = getattr(suite, check_name)
                checks.append(f'{check_name}: {check_func.__doc__}')
            result.append(checks)
        pprint.pprint(result, indent=2)
        return

    # check SSH connectivity
    try:
        logging.info('checking SSH connectivity ...')
        ssh = SshCommander(config['ssh']['user'], config['ssh']['hosts'], config['ssh']['key'])
        ips = ssh.exec_on_all_hosts('sudo -v')
        logging.info({r.ip: 'OK' for r in ips})
    except Exception as e:
        logging.error('could not connect host via SSH', e)
        exit(1)

    # check API connectivity
    try:
        logging.info('checking API connectivity ...')
        api = ApiFetcher(config['api']['fqdn'], config['api']['user'], config['api']['pass'])
        fqdn = api.get_value('cluster', 'name')
        logging.info('successfully connected to {}'.format(fqdn))
    except Exception as e:
        logging.error('could not connect to API via HTTP', e)
        exit(1)

    # render output
    def render_result(_result, _func):
        if not _result[1]:
            pprint.pprint(f'[ ] [{_func.__doc__}] skipped')
            return
        if _result[0] is True:
            to_print = '[+] '
        elif _result[0] is False:
            to_print = '[-] '
        elif _result[0] is None:
            to_print = '[~] '
        elif _result[0] is Exception:
            to_print = '[*] '
        else:
            raise NotImplementedError()

        to_print += f'[{_func.__doc__}] ' + f', '.join([str(k) + ': ' + str(v) for k, v in _result[1].items()])
        pprint.pprint(to_print, width=320)

    # define result callback
    def result_cb(_result, _func, _args, _kwargs):
        if type(_result) == list:
            return [result_cb(r, _func, _args, _kwargs) for r in _result]
        else:
            return render_result(_result, _func)

    # create executor
    executor = CheckExecutor(result_cb)

    # execute single checks
    if args.check != 'all':
        for suite in suites:
            check_names = filter(lambda x: x.startswith('check_') and args.check.lower() in x.lower(), dir(suite))
            for check_name in check_names:
                check_func = getattr(suite, check_name)
                executor.execute(check_func)
        executor.wait()
        executor.shutdown()
        return

    # init statistic collector
    stats_collector = StatsCollector()

    # collect statistics
    def collect_stats(_result):
        if not _result[1]:
            stats_collector.incr_skipped()
        elif _result[0] is True:
            stats_collector.incr_succeeded()
        elif _result[0] is False:
            stats_collector.incr_failed()
        elif _result[0] is None:
            stats_collector.incr_no_result()
        elif _result[0] is Exception:
            stats_collector.incr_errors()
        else:
            raise NotImplementedError()

    # define done callback
    def done_cb(_future):
        result = _future.result()
        [collect_stats(r) for r in result] if type(result) == list else collect_stats(result)

    # execute check suites
    for suite in suites:
        to_print = '[SUITE] {}'.format(suite.__doc__)
        if args.params:
            to_print += ' ' + args.params
        pprint.pprint(to_print)
        params = map(lambda x: x[1], filter(lambda x: args.params in x[0].lower(), suite.params.items()))
        executor.execute_suite(suite, _kwargs=list(params)[0] if args.params else {}, _done_cb=done_cb)
        executor.wait()

    # print statistics
    pprint.pprint("[COLLECTED STATISTICS]")
    pprint.pprint(stats_collector.get_stats())

    # close
    executor.shutdown()


if __name__ == '__main__':
    main()
