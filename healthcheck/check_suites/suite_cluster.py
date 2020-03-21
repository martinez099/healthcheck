import functools
import math
import re

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import GB, to_gb, to_kops
from healthcheck.remote_executor import RemoteExecutor


class Cluster(BaseCheckSuite):
    """Cluster - status, sizing and usage"""

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.api = ApiFetcher.instance(_config)
        self.rex = RemoteExecutor.instance(_config)

    def check_health(self):
        """check cluster health"""
        result = self.api.get('cluster/check')

        return result['cluster_test_result'], result

    def check_shards(self):
        """check cluster shards"""
        rsps = {'shard:{}'.format(shard['uid']):
                self.rex.exec_uni('/opt/redislabs/bin/shard-cli {} PING'.format(shard['uid']),
                                  self.rex.get_targets()[0]) for shard in self.api.get('shards')}

        return all(map(lambda x: x == 'PONG', rsps.values())), rsps

    def check_rladmin_status(self):
        """check if `rladmin status` has errors"""
        rsp = self.rex.exec_uni('sudo /opt/redislabs/bin/rladmin status | grep -v endpoint | grep node',
                                self.rex.get_targets()[0])
        not_ok = re.findall(r'^((?!OK).)*$', rsp, re.MULTILINE)

        return len(not_ok) == 0, {'not OK': len(not_ok)} if not_ok else {'OK': 'all'}

    def check_master_node(self):
        """get master node"""
        rsp = self.rex.exec_uni('sudo /opt/redislabs/bin/rladmin status', self.rex.get_targets()[0])
        found = re.search(r'(^\*?node:\d+\s+master.*$)', rsp, re.MULTILINE)
        parts = re.split(r'\s+', found.group(1))

        return None, {'uid': self.api.get_uid(parts[2]), 'address': parts[2], 'external address': parts[3]}

    def check_license_shards_limit(self):
        """check shards limit in license"""
        number_of_shards = self.api.get_number_of_values('shards')
        _license = self.api.get('license')
        if 'shards_limit' in _license:
            shards_limit = int(_license['shards_limit'])
        else:
            match = re.search(r'Shards limit : (\d+)\n', self.api.get('license')['license'], re.MULTILINE | re.DOTALL)
            shards_limit = int(match.group(1))

        result = shards_limit >= number_of_shards
        kwargs = {'shards limit': shards_limit, 'number of shards': number_of_shards}

        return result, kwargs

    def check_license_expired(self):
        """check if license is expired"""
        expired = self.api.get('license')['expired']

        return not expired, {'license expired': expired}

    def check_sizing(self, **_params):
        """check cluster sizing"""
        number_of_nodes = self.api.get_number_of_values('nodes')
        number_of_cores = self.api.get_sum_of_values('nodes', 'cores')
        total_memory = self.api.get_sum_of_values('nodes', 'total_memory')
        epehemeral_storage_size = self.api.get_sum_of_values('nodes', 'ephemeral_storage_size')
        persistent_storage_size = self.api.get_sum_of_values('nodes', 'persistent_storage_size')

        kwargs = {'number of nodes': str(number_of_nodes),
                  'number of cores': str(number_of_cores),
                  'total memory': '{} GB'.format(to_gb(total_memory)),
                  'ephemeral storage size': '{} GB'.format(to_gb(epehemeral_storage_size)),
                  'persistent storage size': '{} GB'.format(to_gb(persistent_storage_size))}

        if not _params:
            return None, kwargs, "get cluster sizing"

        result = number_of_nodes >= _params['min_nodes'] and number_of_nodes % 2 != 0 and \
            number_of_cores >= _params['min_cores'] and \
            total_memory >= _params['min_memory_GB'] * GB and \
            epehemeral_storage_size >= _params['min_ephemeral_storage_GB'] * GB and \
            persistent_storage_size >= _params['min_persistent_storage_GB'] * GB

        kwargs['number of nodes'] += ' (min: {})'.format(_params['min_nodes'])
        kwargs['number of cores'] += ' (min: {})'.format(_params['min_cores'])
        kwargs['total memory'] += ' (min: {} GB)'.format(_params['min_memory_GB'])
        kwargs['ephemeral storage size'] += ' (min: {} GB)'.format(_params['min_ephemeral_storage_GB'])
        kwargs['persistent storage size'] += ' (min: {} GB)'.format(_params['min_persistent_storage_GB'])

        return result, kwargs

    def check_throughput(self):
        """get throughput of cluster"""
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

        kwargs['min'] = '{} Kops/sec'.format(to_kops(minimum))
        kwargs['avg'] = '{} Kops/sec'.format(to_kops(average))
        kwargs['max'] = '{} Kops/sec'.format(to_kops(maximum))
        kwargs['dev'] = '{} Kops/sec'.format(to_kops(std_dev))

        return None, kwargs

    def check_memory_usage(self):
        """get memory usage of cluster"""
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

        kwargs['min'] = '{} GB'.format(to_gb(total_mem - maximum))
        kwargs['avg'] = '{} GB'.format(to_gb(total_mem - average))
        kwargs['max'] = '{} GB'.format(to_gb(total_mem - minimum))
        kwargs['dev'] = '{} GB'.format(to_gb(std_dev))

        return None, kwargs

    def check_ephemeral_storage_usage(self):
        """get ephemeral storage usage of cluster"""
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

        ephemeral_storage_size = self.api.get_sum_of_values(f'nodes', 'ephemeral_storage_size')

        kwargs['min'] = '{} GB'.format(to_gb(ephemeral_storage_size - maximum))
        kwargs['avg'] = '{} GB'.format(to_gb(ephemeral_storage_size - average))
        kwargs['max'] = '{} GB'.format(to_gb(ephemeral_storage_size - minimum))
        kwargs['dev'] = '{} GB'.format(to_gb(std_dev))

        return None, kwargs

    def check_persistent_storage_usage(self):
        """get persistent storage usage of cluster"""
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

        persistent_storage_size = self.api.get_sum_of_values(f'nodes', 'persistent_storage_size')

        kwargs['min'] = '{} GB'.format(to_gb(persistent_storage_size - maximum))
        kwargs['avg'] = '{} GB'.format(to_gb(persistent_storage_size - average))
        kwargs['max'] = '{} GB'.format(to_gb(persistent_storage_size - minimum))
        kwargs['dev'] = '{} GB'.format(to_gb(std_dev))

        return None, kwargs
