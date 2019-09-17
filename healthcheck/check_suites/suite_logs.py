from healthcheck.check_suites.base_suite import BaseCheckSuite


class LogChecks(BaseCheckSuite):
    """Check logs via API"""

    def check_logs(self, *_args, **_kwargs):
        """get logs"""
        logs = self.api.get('logs')

        kwargs = {'alerts': logs}
        return None, kwargs
