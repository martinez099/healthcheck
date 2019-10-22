import glob
import importlib
import json

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_commander import SshCommander
from healthcheck.render_engine import print_error, print_success


class BaseCheckSuite(object):
    """
    Base Check Suite class.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.api = ApiFetcher(_config['api']['fqdn'], _config['api']['user'], _config['api']['pass'])
        self.ssh = SshCommander(_config['ssh']['hosts'], _config['ssh']['user'], _config['ssh']['key'])
        self.params = {}

    def connectivity_check(self):
        print('')
        self._connectivity_check()
        print('')

    def _connectivity_check(self):
        raise NotImplementedError()

    def _check_ssh_connectivity(self):
        print('checking SSH connectivity ...')
        for ip in self.ssh.hostnames:
            try:
                self.ssh.exec_on_host('sudo -v', ip)
                print_success(f'successfully connected to {ip}')
            except Exception as e:
                print_error(f'could not connect to host {ip}')
                raise e

    def _check_api_connectivity(self):
        try:
            print('checking API connectivity ...')
            fqdn = self.api.get_value('cluster', 'name')
            print_success(f'successfully connected to {fqdn}')
        except Exception as e:
            print_error('could not connect to Redis Enterprise REST-API')
            raise e


def load_params(_dir):
    """
    Load parameter maps.

    :param _dir: The subdirectory of the parameter map.
    :return: A dictionary with the parameters.
    """
    params = {}
    for path in glob.glob(f'healthcheck/check_suites/{_dir}/*.json'):
        with open(path) as file:
            params[path] = json.loads(file.read())
    return params


def load_suites(_args, _config, _base_class=BaseCheckSuite):
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
                    if _args.list or _args.suite and _args.suite.lower() in suite.__doc__.lower():
                        suites.append(suite(_config))
    return suites
