class BaseCheckSuite(object):
    """
    Base Check Suite class.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.params = {}

    def run_connection_checks(self):
        """
        Run connection checks.

        :raise Exception: If connection cannot be established.
        """
        raise NotImplementedError()
