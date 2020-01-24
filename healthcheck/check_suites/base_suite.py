import glob
import json

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_commander import SshCommander
from healthcheck.node_manager import NodeManager


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
        self.nodes = NodeManager.instance(self.api, self.ssh)
        self.params = {}


def load_params(_dir):
    """
    Load parameter maps.

    :param _dir: The subdirectory of the parameter map.
    :return: A dictionary with the parameters.
    """
    params = {}
    for path in glob.glob(f'healthcheck/parameter_maps/{_dir}/*.json'):
        with open(path) as file:
            params[path] = json.loads(file.read())
    return params
