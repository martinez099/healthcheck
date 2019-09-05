from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_commander import SshCommander


class BaseCheckSuite(object):
    """
    Base Check Suite class.
    """

    def __init__(self, _args):
        """
        :param _args: The parsed command line arguments.
        """
        self.api = ApiFetcher(_args.cluster_fqdn, _args.cluster_username, _args.cluster_password)
        self.ssh = SshCommander(_args.ssh_username, _args.ssh_hostnames.split(','), _args.ssh_keyfile)
