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
from healthcheck.printer_funcs import print_list, print_error, print_warning, print_msg
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
    options.add_argument('-c', '--check', help="Specify a check (or CSV list of checks) to execute.", type=str)
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
    Load all check suites.

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


def find_checks(_suites, _args, _config):
    """
    Collect all checks to be executed.

    :param _suites: A list of loaded check suites.
    :param _args: The parsed cmdline arguments.
    :param _config: The parsed configuration.
    :return: A list with found checks to execute.
    """
    checks = []
    for suite in _suites:
        for check in filter(lambda x: x.startswith('check_'), dir(suite)):
            check_func = getattr(suite, check)
            if _args.check:
                check_doc = check_func.__doc__.split('\n')[0].lower()
                check_args = map(lambda x: x.strip(), _args.check.lower().split(','))
                if not list(filter(lambda x: x in check_func.__name__ or x in check_doc, check_args)):
                    continue

            if 'api' not in _config and 'api' in check_func.__code__.co_names:
                continue

            if 'ssh' not in _config and 'docker' not in _config and 'rex' in check_func.__code__.co_names:
                continue

            checks.append((check_func, suite))

    return checks


def load_parameter_map(_suite, _check_func_name, _args):
    """
    Load a parameter map.

    :param _suite: The check suite.
    :param _check_func_name: The check function name.
    :param _args: The parsed arguments.
    :return: A list of tuples.
    """
    if not _args.params:
        return None

    if not _args.check and not _args.suite:
        print_error('sole argument --params not allowed, try using in combination with --suite or --check')
        exit(1)

    if _args.params.endswith('.json'):
        if not os.path.exists(_args.params):
            print_error('could not find parameter map, examine argument of --params')
            exit(1)

        with open(_args.params) as file:
            params = [(_args.params, json.loads(file.read()))]

    else:
        path = f'parameter_maps/{_suite.__class__.__name__.lower()}/{_check_func_name}/'
        loaded_params = {}
        for path in glob.glob(f'{path}*.json'):
            with open(path) as file:
                loaded_params[path] = json.loads(file.read())

        if not loaded_params:
            print_warning(f'- no parameter maps found in {path}\n')
            return None

        params = list(
            filter(lambda x: _args.params.lower() in get_parameter_map_name(x[0].lower()), loaded_params.items()))
        if _args.params and not params:
            print_error('could not find paramter map, choose one: {}'.format(
                list(map(get_parameter_map_name, loaded_params.keys()))))
            exit(1)

        elif len(params) > 1:
            print_error('multiple parameter maps found, choose one: {}'.format(
                list(map(get_parameter_map_name, loaded_params.keys()))))
            exit(1)

    return params


def exec_checks(_suites, _checks, _args, _result_cb, _done_cb=None):
    """
    Execute checks.

    :param _suites: Loaded check suites.
    :param _checks: Found checks.
    :param _args: The parsed arguments.
    :param _result_cb: A result callback.
    :param _done_cb: An optional callback, executed after each check execution.
    """
    executor = CheckExecutor(_result_cb)

    if _args.check and not _checks:
        print_error('could not find a single check, examine argument of --check')
        exit(1)

    elif not _suites:
        print_error('could not find check suite, examine argument of --suite')
        exit(1)

    for check_func, suite in _checks:
        suite.run_connection_checks()
        params = load_parameter_map(suite, check_func.__name__, _args)
        executor.execute(check_func, _params=params[0][1] if params else {}, _done_cb=_done_cb)

    executor.wait()
    executor.shutdown()


def main():

    logging.basicConfig(level=logging.INFO)

    args = parse_args()
    config = parse_config(args)
    suites = load_check_suites(args, config)

    if args.list:
        print_list(suites)
        return

    renderer = import_renderer(config)
    stats_collector = StatsCollector()

    def collect_stats(_future):
        result = _future.result()
        [stats_collector.collect(r) for r in result] if type(result) == list else stats_collector.collect(result)

    def render(_result, _func):
        if type(_result) == list:
            return [render(r, _func) for r in _result]
        else:
            return renderer.render_result(_result, _func, _cluster_name=config['api']['fqdn'])

    checks = find_checks(suites, args, config)
    exec_checks(suites, checks, args, render, collect_stats)
    renderer.render_stats(stats_collector)

    logging.shutdown()

    exit(stats_collector.return_code())
