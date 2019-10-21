import glob
import importlib
import json

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_commander import SshCommander


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

    def load_params(self, _dir):
        """
        Load parameter maps.

        :return: A dictionary with the parameters.
        """
        params = {}
        for path in glob.glob(f'healthcheck/check_suites/{_dir}/*.json'):
            with open(path) as file:
                params[path] = json.loads(file.read())
        self.params = params


def load_suites(_args, _config, _base_class=BaseCheckSuite):
    """
    Load check suites.

    :param _args: The pasred command line arguments.
    :param _config: The configuration.
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
