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
        self.api = ApiFetcher(_config['cluster']['fqdn'], _config['cluster']['user'], _config['cluster']['pass'])
        self.ssh = SshCommander(_config['ssh']['user'], _config['ssh']['hosts'].split(','), _config['ssh']['key'])
