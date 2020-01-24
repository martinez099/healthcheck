import argparse
import configparser
import glob
import importlib
import json
import logging
import os

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.check_executor import CheckExecutor
from healthcheck.stats_collector import StatsCollector
from healthcheck.render_engine import render_result, render_stats, render_list, print_error, print_warning, print_msg
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


def load_check_suites(_args, _config, _base_class=BaseCheckSuite):
    """
    Load check suites.

    :param _args: The pasred command line arguments.
    :param _config: The parsed configuration.
    :param _base_class: The base class of the check suites.
    :return: A list with all instantiated check suites.
    """
    suites = []
    for file in glob.glob('healthcheck/check_suites/suite_*.py'):
        name = file.replace('/', '.').replace('.py', '')
        module = importlib.import_module(name)
        for member in dir(module):
            if member != _base_class.__name__ and not member.startswith('__'):
                suite = getattr(module, member)
                if type(suite) == type.__class__ and issubclass(suite, _base_class):
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
    if not _args.params or not _suite.params:
        return None

    if _args.params.endswith('.json'):
        if not os.path.exists(_args.params):
            print_error("could not find parameter map, examine argument of --params")
            exit(1)

        with open(_args.params) as file:
            params = [(_args.params, json.loads(file.read()))]

    else:
        params = list(filter(lambda x: _args.params.lower() in get_parameter_map_name(x[0].lower()),
                             _suite.params.items()))
        if _args.params and not params:
            print_error('could not find paramter map, available maps: {}'.format(
                list(map(get_parameter_map_name, _suite.params.keys()))))
            # exit(1)

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
    """
    if not _suites:
        print_error('could not find check suite, examine argument of --suite')
        exit(1)

    stats_collector = StatsCollector()

    for suite in _suites:
        print_msg(f'executing check suite: {suite.__doc__}')
        params = load_parameter_map(suite, _args)
        if params:
            print_msg('- using paramter map: {}'.format(get_parameter_map_name(params[0][0])))
        elif suite.params:
            print_warning('no parameter map given, options are: {}'.format(list(map(get_parameter_map_name, suite.params.keys()))))

        #suite.run_connection_checks()

        def collect(_future):
            result = _future.result()
            [stats_collector.collect(r) for r in result] if type(result) == list else stats_collector.collect(result)

        _executor.execute_suite(suite, _kwargs=params[0][1] if params else {}, _done_cb=collect)

        _executor.wait()
        print('')

    render_stats(stats_collector)


def main():

    # configure logging
    logging.basicConfig(level=logging.INFO)

    # parse command line arguments
    args = parse_args()

    # parse configuration file
    config = parse_config(args)

    # load check suites
    suites = load_check_suites(args, config)

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
    if args.check:
        exec_single_checks(suites, args, executor)
    else:
        exec_check_suites(suites, args, executor)

    # shutdown
    executor.shutdown()
    logging.shutdown()
