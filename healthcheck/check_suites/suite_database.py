from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import to_gb, GB


class DatabaseChecks(BaseCheckSuite):
    """Check Database Configuration"""

    def check_bdb_alerts(self, *_args, **_kwargs):
        all_bdb_alerts = self.api.get('bdbs/alerts')
        triggered = [[filter(lambda x: x['state'], alert) for alert in alerts] for alerts in all_bdb_alerts]

        return "check database alerts", None, {'triggered_alerts': triggered}

    def check_memory_size(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        memory_sizes = self.api.get_values('bdbs', 'memory_size')

        kwargs = {f'{bdb_names[i]}': to_gb(memory_sizes[i]) for i in range(0, len(bdb_names))}
        return "get memory size of all databases", None, kwargs

    def check_data_persistence(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        data_persistences = self.api.get_values('bdbs', 'data_persistence')

        kwargs = {f'{bdb_names[i]}': data_persistences[i] for i in range(0, len(bdb_names))}
        return "get persistence setting of all databases", None, kwargs

    def check_rack_awareness(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        rack_awareness = self.api.get_values('bdbs', 'rack_aware')

        kwargs = {f'{bdb_names[i]}': rack_awareness[i] for i in range(0, len(bdb_names))}
        return "get rack awareness setting of all databases", None, kwargs

    def check_reqplica_sync(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        replica_syncs = self.api.get_values('bdbs', 'replica_sync')

        kwargs = {f'{bdb_names[i]}': replica_syncs[i] for i in range(0, len(bdb_names))}
        return "get replica sync setting of all databases", None, kwargs

    def check_sync_sources(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        sync_sources = self.api.get_values('bdbs', 'sync_sources')

        kwargs = {f'{bdb_names[i]}': sync_sources[i] for i in range(0, len(bdb_names))}
        return "get sync sources setting of all databases", None, kwargs

    def check_shards_placement(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        shards_placements = self.api.get_values('bdbs', 'shards_placement')

        kwargs = {f'{bdb_names[i]}': shards_placements[i] for i in range(0, len(bdb_names))}
        return "get shards placement policy of all databases", None, kwargs

    def check_proxy_policy(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        proxy_policies = self.api.get_values('bdbs', 'proxy_policy')

        kwargs = {f'{bdb_names[i]}': proxy_policies[i] for i in range(0, len(bdb_names))}
        return "get proxy policy of all databases", None, kwargs

    def check_replication(self, *_args, **_kwargs):
        bdb_names = self.api.get_values('bdbs', 'name')
        replications = self.api.get_values('bdbs', 'replication')

        kwargs = {f'{bdb_names[i]}': replications[i] for i in range(0, len(bdb_names))}
        return "get replication setting of all databases", None, kwargs

    def check_bdb_alert_settings(self, *_args, **_kwargs):
        alerts = self.api.get('bdbs/alerts')

        return "get database alert settings", None, {'alerts': alerts}
