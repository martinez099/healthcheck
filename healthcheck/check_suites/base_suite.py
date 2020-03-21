class BaseCheckSuite(object):
    """
    Base Check Suite class.
    """

    def run_connection_checks(self):
        """
        Run connection checks.
        """
        if hasattr(self, 'api'):
            self.api.check_connection()

        if hasattr(self, 'rex'):
            self.rex.check_connection()
