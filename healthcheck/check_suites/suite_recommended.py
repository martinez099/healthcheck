from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import format_result, to_gb, GB


class RecommendedChecks(BaseCheckSuite):
    """Check Recommended Requirements"""

    def check_number_of_shards(self, *_args, **_kwargs):
        """check if enough shards"""
        number_of_shards = self.api.get_number_of_shards()
        min_shards = 2

        result = number_of_shards >= min_shards
        return format_result(result, **{'numbe of shards': number_of_shards,
                                        'min shards': min_shards})

    def check_number_of_nodes(self, *_args, **_kwargs):
        """check if enough nodes"""
        number_of_nodes = self.api.get_number_of_nodes()
        min_nodes = 3

        result = number_of_nodes >= min_nodes
        return format_result(result, **{'number of nodes': number_of_nodes,
                                        'min nodes': min_nodes})

    def check_number_of_cores(self, *_args, **_kwargs):
        """check if enough cores"""
        number_of_cores = self.api.get_sum_of_node_values('cores')
        min_cores = 24

        result = number_of_cores >= min_cores
        return format_result(result, **{'number of cores': number_of_cores,
                                        'min cores': min_cores})

    def check_total_memory(self, *_args, **_kwargs):
        """check if enough RAM"""
        total_memory = self.api.get_sum_of_node_values('total_memory')
        min_memory = 90 * GB

        result = total_memory >= min_memory
        return format_result(result, **{'total memory': to_gb(total_memory),
                                        'min memory': to_gb(min_memory)})

    def check_ephemeral_storage(self, *_args, **_kwargs):
        """check if enough ephemeral storage"""
        epehemeral_storage_size = self.api.get_sum_of_node_values('ephemeral_storage_size')
        min_ephemeral_size = 360 * GB

        result = epehemeral_storage_size >= min_ephemeral_size
        return format_result(result, **{'ephemeral storage size': to_gb(epehemeral_storage_size),
                                        'min ephemeral size': to_gb(min_ephemeral_size)})

    def check_persistent_storage(self, *_args, **_kwargs):
        """check if enough persistent storage"""
        persistent_storage_size = self.api.get_sum_of_node_values('persistent_storage_size')
        min_persistent_size = 540 * GB

        result = persistent_storage_size >= min_persistent_size
        return format_result(result, **{'persistent storage size': to_gb(persistent_storage_size),
                                        'min persistent size': to_gb(min_persistent_size)})

    def check_memory_size(self, *_args, **_kwargs):
        """get memory size of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        memory_sizes = self.api.get_bdb_values('memory_size')

        kwargs = {f'{bdb_names[i]}': to_gb(memory_sizes[i]) for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_data_persistence(self, *_args, **_kwargs):
        """get persistence setting of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        data_persistences = self.api.get_bdb_values('data_persistence')

        kwargs = {f'{bdb_names[i]}': data_persistences[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_rack_awareness(self, *_args, **_kwargs):
        """get rack awareness setting of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        rack_awareness = self.api.get_bdb_values('rack_aware')

        kwargs = {f'{bdb_names[i]}': rack_awareness[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_reqplica_sync(self, *_args, **_kwargs):
        """get replica sync setting of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        replica_syncs = self.api.get_bdb_values('replica_sync')

        kwargs = {f'{bdb_names[i]}': replica_syncs[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_sync_sources(self, *_args, **_kwargs):
        """get sync sources setting of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        sync_sources = self.api.get_bdb_values('sync_sources')

        kwargs = {f'{bdb_names[i]}': sync_sources[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_shards_placement(self, *_args, **_kwargs):
        """get shards placement policy of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        shards_placements = self.api.get_bdb_values('shards_placement')

        kwargs = {f'{bdb_names[i]}': shards_placements[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_proxy_policy(self, *_args, **_kwargs):
        """get proxy policy of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        proxy_policies = self.api.get_bdb_values('proxy_policy')

        kwargs = {f'{bdb_names[i]}': proxy_policies[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_replication(self, *_args, **_kwargs):
        """get replication setting of all databases"""
        bdb_names = self.api.get_bdb_values('name')
        replications = self.api.get_bdb_values('replication')

        kwargs = {f'{bdb_names[i]}': replications[i] for i in range(0, len(bdb_names))}
        return format_result(None, **kwargs)

    def check_cluster_and_node_alert_settings(self, *_args, **_kwargs):
        """get cluster and node alert settings"""
        alerts = self.api.get_cluster_value('alert_settings')

        return format_result(None, **{'alerts': alerts})

    def check_bdb_alert_settings(self, *_args, **_kwargs):
        """get database alert settings"""
        alerts = self.api.get_bdb_alerts()

        return format_result(None, **{'alerts': alerts})
