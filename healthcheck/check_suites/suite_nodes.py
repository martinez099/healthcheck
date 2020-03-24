import functools
import math
import re

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import to_gb, to_percent, to_ms
from healthcheck.remote_executor import RemoteExecutor


class Nodes(BaseCheckSuite):
    """
    Check configuration, status and usage of all nodes.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.api = ApiFetcher.instance(_config)
        self.rex = RemoteExecutor.instance(_config)

    def check_nodes_config_001(self, _params):
        """NC-001: Check if log file path is not on root filesystem.

        Executes `df -h /var/opt/redislabs/log` and compares the output to '/dev/root'.

        If this check fails, move the log file path to a mounted filesystem.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('sudo df -h /var/opt/redislabs/log')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        log_file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in log_file_path for log_file_path in log_file_paths])
        kwargs = {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': log_file_path for
                  rsp, log_file_path in zip(rsps, log_file_paths)}

        return result, kwargs

    def check_nodes_config_002(self, _params):
        """NC-002: Check if ephemeral storage path is not on root filesystem.

        Calls '/v1/nodes' from API and gets the ephemeral storage path.
        Executes `df -h /var/opt/redislabs/log` and compares the output to the configured ephemeral storage path.

        If this check fails, move the ephemeral storage path to a mounted filesystem.

        :param _params: None
        :returns: result
        """
        storage_paths = self.api.get_values('nodes', 'ephemeral_storage_path')
        rsps = self.rex.exec_broad(f'sudo df -h {storage_paths[0]}')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in file_paths])
        kwargs = {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': tmp_file_path for
                  rsp, tmp_file_path in zip(rsps, file_paths)}

        return result, kwargs

    def check_nodes_config_003(self, _params):
        """NC-003: Check if persistent storage path is not on root filesystem.

        Calls '/v1/nodes' from API and gets the persistent storage path.
        Executes `df -h /var/opt/redislabs/log` and compares the output to the configured persistent storage path.

        If this check fails, move the persistent storage path to a mounted filesystem.

        :param _params: None
        :returns: result
        """
        storage_paths = self.api.get_values('nodes', 'persistent_storage_path')
        rsps = self.rex.exec_broad(f'sudo df -h {storage_paths[0]}')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in file_paths])
        kwargs = {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': tmp_file_path for
                  rsp, tmp_file_path in zip(rsps, file_paths)}

        return result, kwargs

    def check_nodes_config_004(self, _params):
        """NC-004: Check if swappiness is disabled on each node.

        Executes `grep swap /etc/sysctl.conf || echo inactive` and compares the output to 'inactive'.

        If this check fails, turn off swapping in your OS.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('grep swap /etc/sysctl.conf || echo inactive')
        swappinesses = [rsp.result() for rsp in rsps]

        result = any([swappiness == 'inactive' for swappiness in swappinesses])
        kwargs = {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': swappiness for rsp, swappiness
                  in zip(rsps, swappinesses)}

        return result, kwargs

    def check_nodes_config_005(self, _params):
        """NC-005: Check if THP is disabled on each node.

        Executes `cat /sys/kernel/mm/transparent_hugepage/enabled` and compares the output to 'always madvise [never]'.

        If this check fails, turn off Transparent Huge Pages in your OS.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('cat /sys/kernel/mm/transparent_hugepage/enabled')
        transparent_hugepages = [rsp.result() for rsp in rsps]

        result = all(transparent_hugepage == 'always madvise [never]' for transparent_hugepage in transparent_hugepages)
        kwargs = {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': transparent_hugepage for
                  rsp, transparent_hugepage in zip(rsps, transparent_hugepages)}

        return result, kwargs

    def check_nodes_status_001(self, _params):
        """NS-001: Get OS version of each node.

        Executes `cat /etc/os-release | grep PRETTY_NAME` on each node and outputs found values.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('cat /etc/os-release | grep PRETTY_NAME')
        matches = [re.match(r'^PRETTY_NAME="(.*)"$', rsp.result()) for rsp in rsps]
        os_versions = [match.group(1) for match in matches]

        kwargs = {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': os_version for rsp, os_version
                  in zip(rsps, os_versions)}

        return None, kwargs

    def check_nodes_status_002(self, _params):
        """NS-002: Get RS version of each node.

        Calls '/v1/nodes' and outputs 'software_version' (RE version).

        :param _params: None
        :returns: result
        """
        node_ids = self.api.get_values('nodes', 'uid')
        software_versions = self.api.get_values('nodes', 'software_version')

        kwargs = {f'node:{node_id}': software_version for node_id, software_version in zip(node_ids, software_versions)}

        return None, kwargs

    def check_nodes_status_003(self, _params):
        """NS-003: Check if `rlcheck` has errors.

        Executes `rlcheck` and greps for 'FAILED'.

        If this check fails, follow instructions from `rlcheck` output.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('sudo /opt/redislabs/bin/rlcheck')
        failed = [(re.findall(r'FAILED', rsp.result().strip(), re.MULTILINE), rsp.target) for rsp in rsps]
        errors = sum([len(f[0]) for f in failed])

        return not errors, {f'node:{self.api.get_uid(self.rex.get_addr(f[1]))}': len(f[0]) for f in failed}

    def check_nodes_status_004(self, _params):
        """NS-004: Check if `cnm_ctl status` has errors.

        Executes `cnm_ctl status` and greps for not 'RUNNING'.

        If this check fails, try to restart not running services with `cnm_ctl start <SERVICE>`.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('sudo /opt/redislabs/bin/cnm_ctl status')
        not_running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.target) for rsp in rsps]
        sum_not_running = sum([len(r[0]) for r in not_running])

        return sum_not_running == 0, {f'node:{self.api.get_uid(self.rex.get_addr(n_r[1]))}': len(n_r[0]) for
                                      n_r in not_running}

    def check_nodes_status_005(self, _params):
        """NS-005: Check if `supervisorctl status` has errors.

        Executes `supervisorctl status` and grep for not 'RUNNING'.

        If this check fails, try to restart not running services with `supervisorctl start <SERVICE>`.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('sudo /opt/redislabs/bin/supervisorctl status')
        not_running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.target) for rsp in rsps]
        sum_not_running = sum([len(r[0]) for r in not_running])

        return sum_not_running == 1 * len(rsps), {
            f'node:{self.api.get_uid(self.rex.get_addr(r[1]))}': len(r[0]) - 1 for r in not_running}

    def check_nodes_status_006(self, _params):
        """NS-006: Check if `cat install.log` has errors.

        Executes `grep error /var/opt/redislabs/log/install.log` and counts result.

        If this check fails, try investigating 'install.log'.

        :param _params: None
        :returns: result
        """
        rsps = self.rex.exec_broad('grep error /var/opt/redislabs/log/install.log || echo ""')
        errors = sum([len(rsp.result()) for rsp in rsps])

        return not errors, {f'node:{self.api.get_uid(self.rex.get_addr(rsp.target))}': len(rsp.result()) for
                            rsp in rsps}

    def check_nodes_status_007(self, _params):
        """NS-007: Get network link speed between nodes.

        Executes `ping -c 4 <TARGET>` from all nodes to each node and calculates min/avg/max/dev of RTT.

        :param _params: None
        :returns: result
        """
        cmd_targets = []
        for source in self.rex.get_targets():
            for target, address in self.rex.get_addrs().items():
                if source == target:
                    continue
                cmd_targets.append((f'ping -c 4 {address}', source))

        # calculate averages
        _min, avg, _max, mdev = .0, .0, .0, .0
        futures = self.rex.exec_multi(cmd_targets)
        key = 'rtt min/avg/max/mdev'
        for future in futures:
            lines = future.result().split('\n')
            key = lines[-1:][0].split(' = ')[0]
            parts = lines[-1:][0].split(' = ')[1].split('/')
            _min = min(float(parts[0]), _min) if _min else float(parts[0])
            avg += float(parts[1])
            _max = max(float(parts[2]), _max)
            mdev += float(parts[3].split(' ')[0])

        avg /= len(futures)
        mdev /= len(futures)

        kwargs = {key: '{}/{}/{}/{} ms'.format(to_ms(_min), to_ms(avg), to_ms(_max), to_ms(mdev))}

        return None, kwargs

    def check_nodes_status_008(self, _params):
        """NS-008: Check open TCP ports of each node.

        Does a TCP port scan from all nodes to each node for specified ports: 3333, 3334, 3335, 3336, 3337, 3338, 3339, 8001, 8070, 8080, 8443, 9443 and 36379.
        See https://docs.redislabs.com/latest/rs/administering/designing-production/networking/port-configurations for details.

        If this check fails, investigate network connection between nodes, e.g. firewall rules.

        :param _params: None
        :returns: result
        """
        cmd_targets = []
        ports = [3333, 3334, 3335, 3336, 3337, 3338, 3339, 8001, 8070, 8080, 8443, 9443, 36379]
        cmd = '''python -c "import socket; socket.create_connection(('"'"'{0}'"'"', {1}))" 2> /dev/null || echo '"'"'{0}:{1}'"'"' '''

        for port in ports:
            for source in self.rex.get_targets():
                for external, internal in self.rex.get_addrs().items():
                    if source == external:
                        continue
                    cmd_targets.append((cmd.format(internal, port), source))

        kwargs = {}
        futures = self.rex.exec_multi(cmd_targets)
        for future in futures:
            failed = future.result()
            if failed:
                node_name = f'node:{self.api.get_uid(self.rex.get_addr(future.target))}'
                if node_name not in kwargs:
                    kwargs[node_name] = []
                kwargs[node_name].append(failed)

        return not kwargs, kwargs if kwargs else {'open': 'all'}

    def check_nodes_usage_001(self, _params):
        """NU-001: Check CPU usage (min/avg/max/dev) of each node.

        Calls '/v1/nodes/stats' from API calculates min/avg/max/dev of 1 - 'cpu_idle' (cpu usage).
        It compares to RL recommended values, i.e. maximum of 80%.

        If this check fails, increase CPU power on nodes.

        :param _params: None
        :returns: result
        """
        kwargs = {}
        results = {}

        # get quorum-only node
        nodes = self.api.get('nodes')
        rsps = [self.rex.exec_uni(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                  self.rex.get_targets()[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = list(map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

        for stat in self.api.get('nodes/stats'):
            ints = stat['intervals']
            uid = stat['uid']

            # calculate minimum
            minimum = min((1 - i['cpu_idle']) for i in filter(lambda x: x.get('cpu_idle'), ints))

            # calculate average
            cpu_idles = list(filter(lambda x: x.get('cpu_idle'), ints))
            sum_cpu_usage = sum((1 - i['cpu_idle']) for i in cpu_idles)
            average = sum_cpu_usage/len(cpu_idles)

            # calculate maximum
            maximum = max((1 - i['cpu_idle']) for i in filter(lambda x: x.get('cpu_idle'), ints))

            # calculate std deviation
            q_sum = functools.reduce(lambda x, y: x + pow((1 - y['cpu_idle']) - average, 2), cpu_idles, 0)
            std_dev = math.sqrt(q_sum / len(cpu_idles))

            node_name = f'node:{uid}'
            if uid in quorum_onlys:
                node_name += ' (quorum only)'

            results[node_name] = maximum > .8
            kwargs[node_name] = '{}/{}/{}/{} %'.format(to_percent(minimum * 100),
                                                       to_percent(average * 100),
                                                       to_percent(maximum * 100),
                                                       to_percent(std_dev * 100))

        return not any(results.values()), kwargs

    def check_nodes_usage_002(self, _params):
        """NU-002: Check RAM usage (min/avg/max/dev) of each node.

        Call '/v1/nodes/stats' and calculates min/avg/max/dev of 'total_memory' - 'free_memory' (used memory).
        It compares them to RL recommended values, i.e. maximum of 2/3.

        If this check fails, increase RAM on nodes.

        :param _params: None
        :returns: result
        """
        kwargs = {}
        results = {}

        # get quorum-only node
        nodes = self.api.get('nodes')
        rsps = [self.rex.exec_uni(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                  self.rex.get_targets()[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = list(map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

        for stat in self.api.get('nodes/stats'):
            ints = stat['intervals']
            uid = stat['uid']

            # calculate minimum
            minimum = min(i['free_memory'] for i in filter(lambda x: x.get('free_memory'), ints))

            # calculate average
            free_mems = list(filter(lambda x: x.get('free_memory'), ints))
            sum_free_mem = sum(i['free_memory'] for i in free_mems)
            average = sum_free_mem/len(free_mems)

            # calculate maximum
            maximum = max(i['free_memory'] for i in filter(lambda x: x.get('free_memory'), ints))

            # calculate std deviation
            q_sum = functools.reduce(lambda x, y: x + pow(y['free_memory'] - average, 2), free_mems, 0)
            std_dev = math.sqrt(q_sum / len(free_mems))

            total_mem = self.api.get_value(f'nodes/{uid}', 'total_memory')

            node_name = f'node:{uid}'
            if uid in quorum_onlys:
                node_name += ' (quorum only)'

            results[node_name] = minimum < (total_mem * 2/3)
            kwargs[node_name] = '{}/{}/{}/{} GB ({}/{}/{}/{} %)'.format(to_gb(total_mem - maximum),
                                                                        to_gb(total_mem - average),
                                                                        to_gb(total_mem - minimum),
                                                                        to_gb(std_dev),
                                                                        to_percent((100 / total_mem) * (total_mem - maximum)),
                                                                        to_percent((100 / total_mem) * (total_mem - average)),
                                                                        to_percent((100 / total_mem) * (total_mem - minimum)),
                                                                        to_percent((100 / total_mem) * std_dev))

        return not any(results.values()), kwargs

    def check_nodes_usage_003(self, _params):
        """NU-003: Get ephemeral storage usage (min/avg/max/dev) of each node.

        Calls '/v1/nodes/stats' and calculates min/avg/max/dev of 'ephemeral_storage_size' - 'ephemeral_storage_avail' (used ephemeral storage).

        :param _params: None
        :returns: result
        """
        kwargs = {}

        # get quorum-only node
        nodes = self.api.get('nodes')
        rsps = [self.rex.exec_uni(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                  self.rex.get_targets()[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = list(
            map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

        for stat in self.api.get('nodes/stats'):
            ints = stat['intervals']
            uid = stat['uid']

            # calculate minimum
            minimum = min(
                i['ephemeral_storage_avail'] for i in filter(lambda x: x.get('ephemeral_storage_avail'), ints))

            # calculate average
            ephemeral_storage_avails = list(filter(lambda x: x.get('ephemeral_storage_avail'), ints))
            sum_ephemeral_storage_avail = sum(i['ephemeral_storage_avail'] for i in ephemeral_storage_avails)
            average = sum_ephemeral_storage_avail / len(ephemeral_storage_avails)

            # calculate maximum
            maximum = max(
                i['ephemeral_storage_avail'] for i in filter(lambda x: x.get('ephemeral_storage_avail'), ints))

            # calculate std deviation
            q_sum = functools.reduce(
                lambda x, y: x + pow(y['ephemeral_storage_avail'] - average, 2),
                ephemeral_storage_avails, 0)
            std_dev = math.sqrt(q_sum / len(ephemeral_storage_avails))

            total_size = self.api.get_value(f'nodes/{uid}', 'ephemeral_storage_size')

            node_name = f'node:{uid}'
            if uid in quorum_onlys:
                node_name += ' (quorum only)'

            kwargs[node_name] = '{}/{}/{}/{} GB ({}/{}/{}/{} %)'.format(to_gb(total_size - maximum),
                                                                        to_gb(total_size - average),
                                                                        to_gb(total_size - minimum),
                                                                        to_gb(std_dev),
                                                                        to_percent((100 / total_size) * (total_size - maximum)),
                                                                        to_percent((100 / total_size) * (total_size - average)),
                                                                        to_percent((100 / total_size) * (total_size - minimum)),
                                                                        to_percent((100 / total_size) * std_dev))

        return None, kwargs

    def check_nodes_usage_004(self, _params):
        """NU-004: Get persistent storage usage (min/avg/max/dev) of each node.

        Calls '/v1/nodes/stats' and calculates min/avg/max/dev of 'persistent_storage_size' - 'persistent_storage_avail' (used persistent storage).

        :param _params: None
        :returns: result
        """
        kwargs = {}

        # get quorum-only node
        nodes = self.api.get('nodes')
        rsps = [self.rex.exec_uni(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                  self.rex.get_targets()[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = list(
            map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

        for stat in self.api.get('nodes/stats'):
            ints = stat['intervals']
            uid = stat['uid']

            # calculate minimum
            minimum = min(
                i['persistent_storage_avail'] for i in filter(lambda x: x.get('persistent_storage_avail'), ints))

            # calculate average
            persistent_storage_avails = list(filter(lambda x: x.get('persistent_storage_avail'), ints))
            sum_persistent_storage_avail = sum(i['persistent_storage_avail'] for i in persistent_storage_avails)
            average = sum_persistent_storage_avail / len(persistent_storage_avails)

            # calculate maximum
            maximum = max(
                i['persistent_storage_avail'] for i in filter(lambda x: x.get('persistent_storage_avail'), ints))

            # calculate std deviation
            q_sum = functools.reduce(
                lambda x, y: x + pow(y['persistent_storage_avail'] - average, 2),
                persistent_storage_avails, 0)
            std_dev = math.sqrt(q_sum / len(persistent_storage_avails))

            total_size = self.api.get_value(f'nodes/{uid}', 'persistent_storage_size')

            node_name = f'node:{uid}'
            if uid in quorum_onlys:
                node_name += ' (quorum only)'

            kwargs[node_name] = '{}/{}/{}/{} GB ({}/{}/{}/{} %)'.format(to_gb(total_size - maximum),
                                                                        to_gb(total_size - average),
                                                                        to_gb(total_size - minimum),
                                                                        to_gb(std_dev),
                                                                        to_percent((100 / total_size) * (total_size - maximum)),
                                                                        to_percent((100 / total_size) * (total_size - average)),
                                                                        to_percent((100 / total_size) * (total_size - minimum)),
                                                                        to_percent((100 / total_size) * std_dev))

        return None, kwargs
