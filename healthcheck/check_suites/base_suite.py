from healthcheck.api_fetcher import ApiFetcher
from healthcheck.common import exec_cmd
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
        nodes = self.api.get('nodes')
        addresses = self.ssh.exec_on_all_nodes('hostname -I')
        uid_addrs = [(node['uid'], node['addr']) for node in nodes]
        uid_addrs.sort(key=lambda x: x[0])

        result = all([addresses[i] == uid_addrs[i][1] for i in range(0, len(nodes))])
        kwargs = {'node_{}'.format(uid): address for uid, address in uid_addrs}
        return "check configured hosts", result, kwargs

    def check_hosts(self, *_args, **_kwargs):
        [exec_cmd(f'ping -qont 1 {hostname}') for hostname in self.ssh.hostnames]

        kwargs = {hostname: True for hostname in self.ssh.hostnames}
        return "check host reachabilities", True, {'hostnames': kwargs}
