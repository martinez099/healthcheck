from healthcheck.check_suites.base_suite import BaseCheckSuite


class LogChecks(BaseCheckSuite):
    """Check logs"""

    def check_warnings(self, *_args, **_kwargs):
        """get warnings"""
        logs = self.api.get('logs')

        result = []
        for log in logs:
            if log['severity'] == 'WARNING':
                result.append(log)

        return not len(result), {'warnings': len(result)}

    def check_errors(self, *_args, **_kwargs):
        """get errors"""
        logs = self.api.get('logs')

        result = []
        for log in logs:
            if log['severity'] == 'ERROR':
                result.append(log)

        return not len(result), {'errors': len(result)}
