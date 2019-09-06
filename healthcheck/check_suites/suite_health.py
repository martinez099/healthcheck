from healthcheck.check_suites.base_suite import BaseCheckSuite

from healthcheck.common import format_result


class HealthChecks(BaseCheckSuite):
    """Check Cluster Health"""

    def check_cluster_and_node_alerts(self, *_args, **_kwargs):
        alerts = self.api.get_cluster_value('alert_settings')

        return format_result(None, **{'alerts': alerts})

    def check_bdb_alerts(self, *_args, **_kwargs):
        all_bdb_alerts = self.api.get_bdb_alerts()
        triggered = [[filter(lambda x: x['state'], alert) for alert in alerts] for alerts in all_bdb_alerts]

        return format_result(None, **{'triggered_alerts': triggered})

    def check_rladmin_status(self, *_args, **_kwargs):
        """get output of rladmin status extra all"""
        status = self.ssh.run_rladmin_status()

        return format_result(None, **{'rladmin status extra all': status})

    def check_rlcheck_result(self, *_args, **_kwargs):
        """get output of rlcheck status"""
        check = self.ssh.run_rlcheck()

        return format_result(None, **{'rlcheck': check})

    def check_cnm_ctl_status(self, *_args, **_kwargs):
        """get output of cnm_ctl status"""
        status = self.ssh.run_cnm_ctl_status()

        return format_result(None, **{'cnm_ctl status': status})

    def check_supervisorctl_status(self, *_args, **_kwargs):
        """get output of supervisorctl status"""
        status = self.ssh.run_supervisorctl_status()

        return format_result(None, **{'supervisorctl status': status})

    def check_errors_in_syslog(self, *_args, **_kwargs):
        """get errors in syslog"""
        errors = self.ssh.find_errors_in_syslog()

        return format_result(None, **{'syslog errors': errors})

    def check_errors_in_install_log(self, *_args, **_kwargs):
        """get errors in install.log"""
        errors = self.ssh.find_errors_in_install_log()

        return format_result(None, **{'install.log errors': errors})
