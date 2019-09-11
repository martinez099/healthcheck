from healthcheck.check_suites.base_suite import BaseCheckSuite


class AlertChecks(BaseCheckSuite):
    """Check alerts via API"""

    def check_cluster_and_node_alert_settings(self, *_args, **_kwargs):
        """get cluster and node alert settings"""
        alerts = self.api.get_value('cluster', 'alert_settings')

        kwargs = {'alerts': alerts}
        return None, kwargs

    def check_bdb_alerts(self, *_args, **_kwargs):
        """check database alerts"""
        all_bdb_alerts = self.api.get('bdbs/alerts')
        triggered = [[filter(lambda x: x['state'], alert) for alert in alerts] for alerts in all_bdb_alerts]

        return None, {'triggered_alerts': triggered}

    def check_bdb_alert_settings(self, *_args, **_kwargs):
        """get database alert settings"""
        alerts = self.api.get('bdbs/alerts')

        return None, {'alerts': alerts}
