import functools
import math
import re

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import GB, to_gb, to_kops, to_percent
from healthcheck.remote_executor import RemoteExecutor


class Cluster(BaseCheckSuite):
    """
    Check configuration, status and usage of the cluster.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.api = ApiFetcher.instance(_config)
        self.rex = RemoteExecutor.instance(_config)

    def check_cluster_config_001(self, _params):
        """CC-001: Check cluster sizing.

        Calls '/v1/nodes' from API and compares values with passed paramters.
        If no parameters were passed, only outputs found values.

        Remedy: Upgradinge your nodes.

        :param _params: A dict with cluster sizing values to compare to, see 'parameter_maps/cluster/check_sizing' for examples.
        :returns: result
        """
        number_of_nodes = self.api.get_number_of_values('nodes')
        number_of_cores = self.api.get_sum_of_values('nodes', 'cores')
        total_memory = self.api.get_sum_of_values('nodes', 'total_memory')
        epehemeral_storage_size = self.api.get_sum_of_values('nodes', 'ephemeral_storage_size')
        persistent_storage_size = self.api.get_sum_of_values('nodes', 'persistent_storage_size')

        if not _params:
            kwargs = {'number of nodes': str(number_of_nodes),
                      'number of cores': str(number_of_cores),
                      'total memory': '{} GB'.format(to_gb(total_memory)),
                      'ephemeral storage size': '{} GB'.format(to_gb(epehemeral_storage_size)),
                      'persistent storage size': '{} GB'.format(to_gb(persistent_storage_size))}

            return None, kwargs, "CC-001: Get cluster sizing."

        kwargs = {}
        if number_of_nodes >= _params['min_nodes'] and number_of_nodes % 2 != 0:
            kwargs['number of nodes'] += ' (min: {})'.format(_params['min_nodes'])

        if number_of_cores >= _params['min_cores']:
            kwargs['number of cores'] += ' (min: {})'.format(_params['min_cores'])

        if total_memory >= _params['min_memory_GB'] * GB:
            kwargs['total memory'] += ' (min: {} GB)'.format(_params['min_memory_GB'])

        if epehemeral_storage_size >= _params['min_ephemeral_storage_GB'] * GB:
            kwargs['ephemeral storage size'] += ' (min: {} GB)'.format(_params['min_ephemeral_storage_GB'])

        if persistent_storage_size >= _params['min_persistent_storage_GB'] * GB:
            kwargs['persistent storage size'] += ' (min: {} GB)'.format(_params['min_persistent_storage_GB'])

        return not bool(kwargs), kwargs

    def check_cluster_config_002(self, _params):
        """CC-002: Get master node.

        Executes `rladmin status` on one of the cluster nodes and greps for the master node.
        Outputs UID, internal and external address.

        :param _params: None
        :returns: result
        """
        rsp = self.rex.exec_uni('sudo /opt/redislabs/bin/rladmin status', self.rex.get_targets()[0])
        found = re.search(r'(^\*?node:\d+\s+master.*$)', rsp, re.MULTILINE)
        parts = re.split(r'\s+', found.group(1))

        return None, {'uid': self.api.get_uid(parts[2]), 'address': parts[2], 'external address': parts[3]}

    def check_cluster_status_001(self, _params):
        """CS-001: Check cluster health.

        Calls '/v1/cluster/check' from API and outputs the result.

        Remedy: Investigate the failed node, i.e. run `rladmin status`, grep log files for errors, etc.

        :param _params: None
        :returns: result
        """
        result = self.api.get('cluster/check')

        return result['cluster_test_result'], result

    def check_cluster_status_002(self, _params):
        """CS-002: Check cluster shards.

        Calls '/v1/shards' from API and executes `shard-cli <UID> PING` for every shard UID on one of the cluster nodes.
        Collects the responses and compares it against 'PONG'.

        Remedy: Investigate the failed shard, i.e. grep log files for errors.

        :param _params: None
        :returns: result
        """
        rsps = {f'shard:{shard["uid"]}': self.rex.exec_uni(f'/opt/redislabs/bin/shard-cli {shard["uid"]} PING',
                                                           self.rex.get_targets()[0]) for shard in self.api.get('shards')}
        kwargs = dict(filter(lambda x: x[1] != 'PONG', rsps.items()))

        return not kwargs, kwargs if kwargs else {'OK': 'all'}

    def check_cluster_status_003(self, _params):
        """CS-003: Check if `rladmin status` has errors.

        Executes `rladmin status | grep -v endpoint | grep node` on one of the cluster nodes.
        Collects output and compares against 'OK'.

        Remedy: Investigate the failed node, i.e. grep log files for errors.

        :param _params: None
        :returns: result
        """
        rsp = self.rex.exec_uni('sudo /opt/redislabs/bin/rladmin status | grep -v endpoint | grep node',
                                self.rex.get_targets()[0])
        not_ok = re.findall(r'^((?!OK).)*$', rsp, re.MULTILINE)

        return len(not_ok) == 0, {'not OK': len(not_ok)} if not_ok else {'OK': 'all'}

    def check_cluster_status_004(self, _params):
        """CS-004: Check license.

        Calls '/v1/license' from API and compares the shards limit with actual shards count and checks expired field.

        Remedy: Update your license.

        :param _params: None
        :returns: result
        """
        number_of_shards = self.api.get_number_of_values('shards')
        _license = self.api.get('license')
        expired = _license['expired']
        if 'shards_limit' in _license:
            shards_limit = int(_license['shards_limit'])
        else:
            match = re.search(r'Shards limit : (\d+)\n', self.api.get('license')['license'], re.MULTILINE | re.DOTALL)
            shards_limit = int(match.group(1))

        result = shards_limit >= number_of_shards and not expired
        kwargs = {'shards limit': shards_limit, 'number of shards': number_of_shards, 'expired': expired}

        return result, kwargs

    def check_cluster_usage_001(self, _params):
        """CU-001: Get throughput of cluster.

        Calls '/v1/cluster/stats' from API and calculates min/avg/max/dev of 'total_req' (total requests per second).

        :param _params: None
        :returns: result
        """
        kwargs = {}
        stats = self.api.get('cluster/stats')

        # calculate minimum
        minimum = min([i['total_req'] for i in filter(lambda x: x.get('total_req'), stats['intervals'])])

        # calculate average
        total_reqs = list(filter(lambda x: x.get('total_req'), stats['intervals']))
        sum_total_req = sum([i['total_req'] for i in total_reqs])
        average = sum_total_req / len(total_reqs)

        # calculate maximum
        maximum = max([i['total_req'] for i in filter(lambda x: x.get('total_req'), stats['intervals'])])

        # calculate std deviation
        q_sum = functools.reduce(lambda x, y: x + pow(y['total_req'] - average, 2), total_reqs, 0)
        std_dev = math.sqrt(q_sum / len(total_reqs))

        kwargs['min'] = '{} Kops'.format(to_kops(minimum))
        kwargs['avg'] = '{} Kops'.format(to_kops(average))
        kwargs['max'] = '{} Kops'.format(to_kops(maximum))
        kwargs['dev'] = '{} Kops'.format(to_kops(std_dev))

        return None, kwargs

    def check_cluster_usage_002(self, _params):
        """CU-002: Get memory usage of cluster.

        Calls '/v1/cluster/stats' from API and calculates min/avg/max/dev of 'total_memory' - 'free_memory' (used memory).

        :param _params: None
        :returns: result
        """
        kwargs = {}
        stats = self.api.get('cluster/stats')

        # calculate minimum
        minimum = min(i['free_memory'] for i in filter(lambda x: x.get('free_memory'), stats['intervals']))

        # calculate average
        free_mems = list(filter(lambda x: x.get('free_memory'), stats['intervals']))
        sum_free_mem = sum(i['free_memory'] for i in free_mems)
        average = sum_free_mem/len(free_mems)

        # calculate maximum
        maximum = max(i['free_memory'] for i in filter(lambda x: x.get('free_memory'), stats['intervals']))

        # calculate std deviation
        q_sum = functools.reduce(lambda x, y: x + pow(y['free_memory'] - average, 2), free_mems, 0)
        std_dev = math.sqrt(q_sum / len(free_mems))

        total_mem = self.api.get_sum_of_values('nodes', 'total_memory')

        kwargs['min'] = '{} GB ({} %)'.format(to_gb(total_mem - maximum), to_percent((100 / total_mem) * (total_mem - maximum)))
        kwargs['avg'] = '{} GB ({} %)'.format(to_gb(total_mem - average), to_percent((100 / total_mem) * (total_mem - average)))
        kwargs['max'] = '{} GB ({} %)'.format(to_gb(total_mem - minimum), to_percent((100 / total_mem) * (total_mem - minimum)))
        kwargs['dev'] = '{} GB ({} %)'.format(to_gb(std_dev), to_percent((100 / total_mem) * std_dev))

        return None, kwargs

    def check_cluster_usage_003(self, _params):
        """CU-003: Get ephemeral storage usage of cluster.

        Calls '/v1/cluster/stats' from API and calculates
        min/avg/max/dev of 'ephemeral_storage_size' - 'ephemeral_storage_avail' (used ephemeral storage).

        :param _params: None
        :returns: result
        """
        kwargs = {}
        stats = self.api.get('cluster/stats')

        # calculate minimum
        minimum = min([i['ephemeral_storage_avail'] for i in
                       filter(lambda x: x.get('ephemeral_storage_avail'), stats['intervals'])])

        # calculate average
        ephemeral_storage_avails = list(filter(lambda x: x.get('ephemeral_storage_avail'), stats['intervals']))
        sum_ephemeral_storage_avail = sum(i['ephemeral_storage_avail'] for i in ephemeral_storage_avails)
        average = sum_ephemeral_storage_avail / len(ephemeral_storage_avails)

        # calculate maximum
        maximum = max([i['ephemeral_storage_avail'] for i in
                       filter(lambda x: x.get('ephemeral_storage_avail'), stats['intervals'])])

        # calculate std deviation
        q_sum = functools.reduce(lambda x, y: x + pow(y['ephemeral_storage_avail'] - average, 2),
                                 ephemeral_storage_avails, 0)
        std_dev = math.sqrt(q_sum / len(ephemeral_storage_avails))

        total_size = self.api.get_sum_of_values(f'nodes', 'ephemeral_storage_size')

        kwargs['min'] = '{} GB ({} %)'.format(to_gb(total_size - maximum), to_percent((100 / total_size) * (total_size - maximum)))
        kwargs['avg'] = '{} GB ({} %)'.format(to_gb(total_size - average), to_percent((100 / total_size) * (total_size - average)))
        kwargs['max'] = '{} GB ({} %)'.format(to_gb(total_size - minimum), to_percent((100 / total_size) * (total_size - minimum)))
        kwargs['dev'] = '{} GB ({} %)'.format(to_gb(std_dev), to_percent((100 / total_size) * std_dev))

        return None, kwargs

    def check_cluster_usage_004(self, _params):
        """CU-004: Get persistent storage usage of cluster.

        Calls '/v1/cluster/stats' from API and calculates
        min/avg/max/dev of 'persistent_storage_size' - 'persistent_storage_avail' (used persistent storage).

        :param _params: None
        :returns: result
        """
        kwargs = {}
        stats = self.api.get('cluster/stats')

        # calculate minimum
        minimum = min([i['persistent_storage_avail'] for i in
                       filter(lambda x: x.get('persistent_storage_avail'), stats['intervals'])])

        # calculate average
        persistent_storage_avails = list(filter(lambda x: x.get('persistent_storage_avail'), stats['intervals']))
        sum_persistent_storage_avail = sum(i['persistent_storage_avail'] for i in persistent_storage_avails)
        average = sum_persistent_storage_avail / len(persistent_storage_avails)

        # calculate maximum
        maximum = max([i['persistent_storage_avail'] for i in
                       filter(lambda x: x.get('persistent_storage_avail'), stats['intervals'])])

        # calculate std deviation
        q_sum = functools.reduce(lambda x, y: x + pow(y['persistent_storage_avail'] - average, 2),
                                 persistent_storage_avails, 0)
        std_dev = math.sqrt(q_sum / len(persistent_storage_avails))

        total_size = self.api.get_sum_of_values(f'nodes', 'persistent_storage_size')

        kwargs['min'] = '{} GB ({} %)'.format(to_gb(total_size - maximum), to_percent((100 / total_size) * (total_size - maximum)))
        kwargs['avg'] = '{} GB ({} %)'.format(to_gb(total_size - average), to_percent((100 / total_size) * (total_size - average)))
        kwargs['max'] = '{} GB ({} %)'.format(to_gb(total_size - minimum), to_percent((100 / total_size) * (total_size - minimum)))
        kwargs['dev'] = '{} GB ({} %)'.format(to_gb(std_dev), to_percent((100 / total_size) * std_dev))

        return None, kwargs
