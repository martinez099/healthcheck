from healthcheck.api_fetcher import ApiFetcher
from healthcheck.common_funcs import is_api_configured, is_rex_configured
from healthcheck.remote_executor import RemoteExecutor


class BaseCheckSuite(object):
    """
    Base Check Suite class.

    All check suites must derive from this class.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.config = _config

    def api(self):
        """
        Get an API instance.

        :return: ApiFetcher
        """
        return ApiFetcher.inst(self.config)

    def rex(self):
        """
        Get an Remote Executor instance.

        :return: RemoteExecutor
        """
        return RemoteExecutor.inst(self.config)

    def run_connection_checks(self):
        """
        Run connection checks.
        """
        if is_api_configured(self.config):
            self.api().check_connection()

        if is_rex_configured(self.config):
            self.rex().check_connection()
