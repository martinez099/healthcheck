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
        self.ssh = SshCommander(_config['ssh']['user'], _config['ssh']['hosts'].split(','), _config['ssh']['key'])

    def check_hostnames(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_values('nodes')

        kwargs = {'number of nodes': number_of_nodes}
        return "check number of configured hosts", len(self.ssh.hostnames) == number_of_nodes, kwargs

    def check_hosts(self, *_args, **_kwargs):
        [self.ssh.exec_cmd(f'ping -qont 1 {hostname}') for hostname in self.ssh.hostnames]

        kwargs = {hostname: True for hostname in self.ssh.hostnames}
        return "check host reachabilities", True, {'hostnames': kwargs}
