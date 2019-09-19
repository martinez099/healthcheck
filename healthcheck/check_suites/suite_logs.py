from healthcheck.check_suites.base_suite import BaseCheckSuite


class LogChecks(BaseCheckSuite):
    """Check logs via API"""

    def check_warnings(self, *_args, **_kwargs):
        """get warnings"""
        logs = self.api.get('logs')

        kwargs = {'warnings': []}
        for log in logs:
            if log['severity'] == 'WARNING':
                kwargs['warnings'].append(log)

        return None, kwargs

    def check_errors(self, *_args, **_kwargs):
        """get errors"""
        logs = self.api.get('logs')

        kwargs = {'errors': []}
        for log in logs:
            if log['severity'] == 'ERROR':
                kwargs['errors'].append(log)

        return None, kwargs
