from healthcheck.check_suites.base_suite import BaseCheckSuite


class AlertChecks(BaseCheckSuite):
    """Check triggered alerts"""

    def check_bdb_alerts(self, *_args, **_kwargs):
        """check database alerts"""
        all_bdb_alerts = self.api.get('bdbs/alerts')
        triggered = {}
        for alerts_k, alerts_v in all_bdb_alerts.items():
            for alert_k, alert_v in alerts_v.items():
                if alert_v['state']:
                    triggered[alert_k] = alert_v

        return not len(triggered), {'triggered_alerts': triggered}
