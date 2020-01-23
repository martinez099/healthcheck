import glob
import json

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_commander import SshCommander
from healthcheck.render_engine import print_error, print_success, print_msg


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

    def run_connection_checks(self):
        """
        Run connectivity checks.

        :return:
        """
        raise NotImplementedError()

    def _check_ssh_connectivity(self):
        print_msg('checking SSH connectivity')
        for ip in self.ssh.hostnames:
            try:
                self.ssh.exec_on_host('sudo -v', ip)
                print_success(f'successfully connected to {ip}')
            except Exception as e:
                print_error(f'could not connect to host {ip}:', e)
                exit(2)

    def _check_api_connectivity(self):
        try:
            print_msg('checking API connectivity')
            fqdn = self.api.get_value('cluster', 'name')
            print_success(f'successfully connected to {fqdn}')
        except Exception as e:
            print_error('could not connect to Redis Enterprise REST-API:', e)
            exit(3)


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
