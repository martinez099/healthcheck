import argparse
import configparser
import glob
import importlib
import json
import logging
import os

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.check_executor import CheckExecutor
from healthcheck.common_funcs import get_parameter_map_name
from healthcheck.printer_funcs import print_list, print_error, print_warning, print_msg, print_success
from healthcheck.stats_collector import StatsCollector


def parse_args():
    """
    Parse command line arguments.

    :return: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()

    options = parser.add_argument_group()
    options.add_argument('-l', '--list', help="List all check suites.", action='store_true')
    options.add_argument('-s', '--suite', help="Specify a suite to execute.", type=str)
    options.add_argument('-c', '--check', help="Specify a check to execute.", type=str)
    options.add_argument('-p', '--params', help="Specify a parameter map to use.", type=str)
    options.add_argument('-cfg', '--config', help="Path to config file", type=str, default='config.ini')

    return parser.parse_args()


def parse_config(_args):
    """
    Parse configuration file.

    :return: The parsed configuration.
    """
    if not os.path.isfile(_args.config):
        print_error('could not find configuration file, examine argument of --config')
        exit(1)

    config = configparser.ConfigParser()
    with open(_args.config, 'r') as configfile:
        config.read_file(configfile)

    return config


def import_renderer(config):
    """
    Load configured renderer module, falls back to 'basic'.

    :param config: A parsed configuration file.
    """
    if 'renderer' in config and 'module' in config['renderer']:
        renderer = config['renderer']['module']
    else:
        renderer = 'basic'

    return importlib.import_module('healthcheck.result_renderers.{}_renderer'.format(renderer))


def load_check_suites(_args, _config, _check_connection=True):
    """
    Load check suites.

    :param _args: The pasred command line arguments.
    :param _config: The parsed configuration.
    :param _check_connection: Run connection checks, defaults to True.
    :return: A list with all instantiated check suites.
    """
    suites = []
    for file in glob.glob('healthcheck/check_suites/suite_*.py'):
        name = file.replace('/', '.').replace('.py', '')
        module = importlib.import_module(name)
        for member in dir(module):
            if member != BaseCheckSuite.__name__ and not member.startswith('__'):
                suite = getattr(module, member)
                if type(suite) == type.__class__ and issubclass(suite, BaseCheckSuite):
                    if not _args.suite or _args.suite and _args.suite.lower() in suite.__doc__.lower():
                        suites.append(suite(_config))

    return suites


def load_parameter_map(_suite, _args):
    """
    Load parameter map.

    :param _suite: The check suite.
    :param _args: The parsed arguments.
    :return: A list of tuples.
    """
    if not _args.params:
        return None

    if not _args.check and not _args.suite:
        print_error('sole argument --params not allowed, try using in combination with --suite or --check')
        exit(1)

    if not _suite.params:
        print_warning('- suite does not support parameters\n')
        return None

    if _args.params.endswith('.json'):
        if not os.path.exists(_args.params):
            print_error('could not find parameter map, examine argument of --params')
            exit(1)

        with open(_args.params) as file:
            params = [(_args.params, json.loads(file.read()))]

    else:
        params = list(filter(lambda x: _args.params.lower() in get_parameter_map_name(x[0].lower()),
                             _suite.params.items()))
        if _args.params and not params:
            print_error('could not find paramter map, choose one: {}'.format(
                list(map(get_parameter_map_name, _suite.params.keys()))))
            exit(1)

        elif len(params) > 1:
            print_error('multiple parameter maps found, choose one: {}'.format(
                list(map(get_parameter_map_name, _suite.params.keys()))))
            exit(1)

    return params


def exec_single_checks(_suites, _args, _executor):
    """
    Execute single checks.

    :param _suites: The loaded check suites.
    :param _args: The parsed arguments.
    :param _executor: The check executor.
    """
    checks = []
    for suite in _suites:
        for check in filter(lambda x: x.startswith('check_'), dir(suite)):
            check_func = getattr(suite, check)
            if _args.check and _args.check.lower() not in check_func.__doc__.lower():
                continue
            checks.append((check_func, suite))

    if not checks:
        print_error('could not find a single check, examine argument of --check')
        exit(1)

    for check, suite in checks:
        params = load_parameter_map(suite, _args)
        _executor.execute(check, _kwargs=params[0][1] if params else {})

    _executor.wait()


def exec_check_suites(_suites, _args, _executor):
    """
    Execute check suites.

    :param _suites: The loaded check suites.
    :param _args: The parsed arguments.
    :param _executor: The check executor.
    :return: Collected statistics.
    """
    if not _suites:
        print_error('could not find check suite, examine argument of --suite')
        exit(1)

    stats_collector = StatsCollector()

    for suite in _suites:
        print_msg(f'executing check suite: {suite.__doc__}')
        params = load_parameter_map(suite, _args)
        if params:
            print_success('- using paramter map: {}\n'.format(get_parameter_map_name(params[0][0])))
        elif suite.params:
            print_warning('- no parameter map given, options are: {}\n'.format(list(map(get_parameter_map_name, suite.params.keys()))))

        suite.run_connection_checks()

        def collect(_future):
            result = _future.result()
            [stats_collector.collect(r) for r in result] if type(result) == list else stats_collector.collect(result)

        _executor.execute_suite(suite, _kwargs=params[0][1] if params else {}, _done_cb=collect)

        _executor.wait()
        print_msg('')

    return stats_collector


def main():

    # configure logging
    logging.basicConfig(level=logging.INFO)

    # parse command line arguments
    args = parse_args()

    # parse configuration file
    config = parse_config(args)

    # load configured renderer
    renderer = import_renderer(config)

    # load suites
    suites = load_check_suites(args, config)

    # list suites
    if args.list:
        print_list(suites)
        return

    # render result
    def render(_result, _func, _args, _kwargs):
        if type(_result) == list:
            return [render(r, _func, _args, _kwargs) for r in _result]
        else:
            return renderer.render_result(_result, _func)

    # execute checks
    executor = CheckExecutor(render)
    if args.check:
        exec_single_checks(suites, args, executor)
    else:
        stats = exec_check_suites(suites, args, executor)
        renderer.render_stats(stats)

    # shutdown
    executor.shutdown()
    logging.shutdown()
