import re

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import calc_usage, parse_semver, to_gb, to_percent, to_ms


class Nodes(BaseCheckSuite):
    """
    Check configuration, status and usage of all nodes.
    """

    def _get_quorum_only_nodes(self):
        """
        Get UID of nodes marked 'quorum only'.

        :return: A list of node:UIDs.
        """
        nodes = self.api().get('nodes')
        rsps = [self.rex().exec_uni(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                  self.rex().get_targets()[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]

        return list(map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

    def _get_file_systems(self, _path):
        """
        Get the filesystem of each node on which the given filepath is stored.

        :param _path: The file path.
        :return: A dict mapping node:UID -> filesystem
        """
        rsps = self.rex().exec_broad(f'sudo df -h {_path}')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        fsystems = [match.group(1) for match in matches]

        return {f'node:{self.api().get_uid(self.rex().get_addr(rsp.target))}': fsystem for rsp, fsystem in
                zip(rsps, fsystems)}

    def check_nodes_config_001(self, _params):
        """NC-001: Check if `rlcheck` has errors.

        Executes `rlcheck` and greps for 'FAILED'.

        Remedy: Follow instructions from `rlcheck` output.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('sudo /opt/redislabs/bin/rlcheck')
        failed = [(re.findall(r'FAILED', rsp.result().strip(), re.MULTILINE), rsp.target) for rsp in rsps]
        errors = sum([len(f[0]) for f in failed])

        return not errors, {f'node:{self.api().get_uid(self.rex().get_addr(f[1]))}': len(f[0]) for f in failed}

    def check_nodes_config_002(self, _params):
        """NC-002: Check if log file path is not on the root filesystem.

        Executes `df -h /var/opt/redislabs/log` and compares the output to '/dev/root'.

        Remedy: Move the log file path to a mounted filesystem.

        :param _params: None
        :returns: result
        """
        api = True  # API is called in subroutine
        filesystems = self._get_file_systems('/var/opt/redislabs/log')
        result = all(['/dev/root' not in fsystem for fsystem in filesystems.values()])

        return result, filesystems

    def check_nodes_config_003(self, _params):
        """NC-003: Check if ephemeral storage path is not on the root filesystem.

        Calls '/v1/nodes' from API and gets the ephemeral storage path.
        Executes `df -h /var/opt/redislabs/log` and compares the output to the configured ephemeral storage path.

        Remedy: Move the ephemeral storage path to a mounted filesystem.

        :param _params: None
        :returns: result
        """
        storage_paths = self.api().get_values('nodes', 'ephemeral_storage_path')
        filesystems = self._get_file_systems(storage_paths[0])
        result = all(['/dev/root' not in fsystem for fsystem in filesystems.values()])

        return result, filesystems

    def check_nodes_config_004(self, _params):
        """NC-004: Check if persistent storage path is not on the root filesystem.

        Calls '/v1/nodes' from API and gets the persistent storage path.
        Executes `df -h /var/opt/redislabs/log` and compares the output to the configured persistent storage path.

        Remedy: Move the persistent storage path to a mounted filesystem.

        :param _params: None
        :returns: result
        """
        storage_paths = self.api().get_values('nodes', 'persistent_storage_path')
        filesystems = self._get_file_systems(storage_paths[0])
        result = all(['/dev/root' not in fsystem for fsystem in filesystems.values()])

        return result, filesystems

    def check_nodes_config_005(self, _params):
        """NC-005: Check swaps on each node.

        Executes `wc -l < /proc/swaps` and checks if the output is > 1.

        Remedy: Turn off swapping in your OS by executing `swapoff -a`.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad("wc -l < /proc/swaps")
        swaps = map(lambda x: int(x.result()), rsps)
        result = any([swap <= 1 for swap in swaps])
        info = {f'node:{self.api().get_uid(self.rex().get_addr(rsp.target))}': 'OK' if swap <= 1 else 'FAILED' for rsp, swap in zip(rsps, swaps)}

        return result, info

    def check_nodes_config_006(self, _params):
        """NC-006: Check if THP is disabled on each node.

        Executes `cat /sys/kernel/mm/transparent_hugepage/enabled` and compares the output to 'always madvise [never]'.

        Remedy: Turn off Transparent Huge Pages in your OS.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('cat /sys/kernel/mm/transparent_hugepage/enabled')
        transparent_hugepages = [rsp.result() for rsp in rsps]
        result = all(transparent_hugepage == 'always madvise [never]' for transparent_hugepage in transparent_hugepages)
        info = {f'node:{self.api().get_uid(self.rex().get_addr(rsp.target))}': transparent_hugepage for
                rsp, transparent_hugepage in zip(rsps, transparent_hugepages)}

        return result, info

    def check_nodes_config_007(self, _params):
        """NC-007: Get OS version of each node.

        Executes `cat /etc/os-release | grep PRETTY_NAME` on each node and outputs found values.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('cat /etc/os-release | grep PRETTY_NAME')
        matches = [re.match(r'^PRETTY_NAME="(.*)"$', rsp.result()) for rsp in rsps]
        os_versions = [match.group(1) for match in matches]
        info = {f'node:{self.api().get_uid(self.rex().get_addr(rsp.target))}': os_version for rsp, os_version in
                zip(rsps, os_versions)}

        return None, info

    def check_nodes_config_008(self, _params):
        """NC-008: Check RS version of each node.

        Calls '/v1/nodes' and outputs 'software_version' (RE version).

        Remedy: Upgrade Redis Enterprise software version to 5.6.

        :param _params: None
        :returns: result
        """
        node_ids = self.api().get_values('nodes', 'uid')
        software_versions = self.api().get_values('nodes', 'software_version')
        result = all(map(lambda x: parse_semver(x)[0:2] == (5, 6), software_versions))
        info = {f'node:{node_id}': software_version for node_id, software_version in zip(node_ids, software_versions)}

        return result, info

    def check_nodes_config_009(self, _params):
        """NC-009: Check if `cat install.log` has errors.

        Executes `grep error /var/opt/redislabs/log/install.log` and counts result.

        Remedy: Inevstigate errors in 'install.log'.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('grep error /var/opt/redislabs/log/install.log || echo ""')
        errors = sum([len(rsp.result()) for rsp in rsps])

        return not errors, {f'node:{self.api().get_uid(self.rex().get_addr(rsp.target))}': len(rsp.result()) for
                            rsp in rsps}

    def check_nodes_config_010(self, _params):
        """NC-010: Get network link between nodes.

        Executes `ping -c 4 <TARGET>` from all nodes to each node and calculates min/avg/max/dev of RTT.

        :param _params: None
        :returns: result
        """
        cmd_targets = []
        for source in self.rex().get_targets():
            for target, address in self.rex().get_addrs().items():
                if source == target:
                    continue
                cmd_targets.append((f'ping -c 4 {address}', source))

        # calculate values
        _min, avg, _max, mdev = .0, .0, .0, .0
        futures = self.rex().exec_multi(cmd_targets)
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

        info = {key: '{}/{}/{}/{} ms'.format(to_ms(_min), to_ms(avg), to_ms(_max), to_ms(mdev))}

        return None, info

    def check_nodes_config_011(self, _params):
        """NC-011: Check open TCP ports of each node.

        Does a TCP port scan from all nodes to each node for specified ports:
        3333, 3334, 3335, 3336, 3337, 3338, 3339, 8001, 8070, 8080, 8443, 9443 and 36379.
        See https://docs.redislabs.com/latest/rs/administering/designing-production/networking/port-configurations for details.

        Remedy: Investigate network connection between nodes, e.g. firewall rules.

        :param _params: None
        :returns: result
        """
        cmd_targets = []
        ports = [3333, 3334, 3335, 3336, 3337, 3338, 3339, 8001, 8070, 8080, 8443, 9443, 36379]
        cmd = '''python -c "import socket; socket.create_connection(('"'{0}'"', {1}))" 2> /dev/null || echo '"'{0}:{1}'"' '''

        for port in ports:
            for source in self.rex().get_targets():
                for external, internal in self.rex().get_addrs().items():
                    if source == external:
                        continue
                    cmd_targets.append((cmd.format(internal, port), source))

        info = {}
        futures = self.rex().exec_multi(cmd_targets)
        for future in futures:
            failed = future.result()
            if failed:
                node_name = f'node:{self.api().get_uid(self.rex().get_addr(future.target))}'
                if node_name not in info:
                    info[node_name] = []
                info[node_name].append(failed)

        return not info, info if info else {'OK': 'all'}

    def check_nodes_config_012(self, _params):
        """NC-012: Check if 'vm.overcommit_memory' is set to '1' on each node.

        Executes `cat /proc/sys/vm/overcommit_memory` and compares the output to '1'.

        Remedy: Set 'vm.overcommit_memory=1' in '/etc/sysctl.conf' in your OS.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('cat /proc/sys/vm/overcommit_memory')
        overcommit_mems = [rsp.result() for rsp in rsps]
        result = all(overcommit_mem == '1' for overcommit_mem in overcommit_mems)
        info = {f'node:{self.api().get_uid(self.rex().get_addr(rsp.target))}': overcommit_mem for
                rsp, overcommit_mem in zip(rsps, overcommit_mems)}

        return result, info

    def check_nodes_status_001(self, _params):
        """NS-001: Check if `cnm_ctl status` has errors.

        Executes `cnm_ctl status` and greps for not 'RUNNING'.

        Remedy: Start not running services with `cnm_ctl start <SERVICE>`.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('sudo /opt/redislabs/bin/cnm_ctl status')
        not_running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.target) for rsp in rsps]
        sum_not_running = sum([len(r[0]) for r in not_running])

        return sum_not_running == 0, {f'node:{self.api().get_uid(self.rex().get_addr(n_r[1]))}': len(n_r[0]) for
                                      n_r in not_running}

    def check_nodes_status_002(self, _params):
        """NS-002: Check if `supervisorctl status` has errors.

        Executes `supervisorctl status` and grep for not 'RUNNING'.

        Remedy: Start not running services with `supervisorctl start <SERVICE>`.

        :param _params: None
        :returns: result
        """
        rsps = self.rex().exec_broad('sudo /opt/redislabs/bin/supervisorctl status')
        not_running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.target) for rsp in rsps]
        sum_not_running = sum([len(r[0]) for r in not_running])

        return sum_not_running == 1 * len(rsps), {
            f'node:{self.api().get_uid(self.rex().get_addr(r[1]))}': len(r[0]) - 1 for r in not_running}

    def check_nodes_status_003(self, _params):
        """NS-003: Check node alerts

        Calls '/v1/nodes/alerts' from API and outputs triggered alerts.

        Remedy: Investigate triggered alerts by checking log files.

        :param _params: None
        :returns: result
        """
        alerts = self.api().get('nodes/alerts')
        info = {}
        for uid in alerts:
            enableds = list(filter(lambda x: x[1]['state'], alerts[uid].items()))
            if enableds:
                info['node:{}'.format(uid)] = enableds

        return not info, info

    def check_nodes_usage_001(self, _params):
        """NU-001: Check CPU usage of each node (min/avg/max/dev).

        Calls '/v1/nodes/stats' from API calculates min/avg/max/dev of 1 - 'cpu_idle' (cpu usage).
        It compares to RL recommended values, i.e. maximum of 80%.

        Remedy: Increase CPU power on nodes.

        :param _params: None
        :returns: result
        """
        info = {}
        results = {}
        rex = True  # Remote Executor called in subroutine
        quorum_onlys = self._get_quorum_only_nodes()

        for stats in self.api().get('nodes/stats'):
            minimum, average, maximum, std_dev = calc_usage(stats['intervals'], 'cpu_idle')

            node_name = f'node:{stats["uid"]}'
            if stats['uid'] in quorum_onlys:
                node_name += ' (quorum only)'

            results[node_name] = minimum > .2
            info[node_name] = '{}/{}/{}/{} %'.format(to_percent((1 - maximum) * 100),
                                                     to_percent((1 - average) * 100),
                                                     to_percent((1 - minimum) * 100),
                                                     to_percent(std_dev * 100))

        return all(results.values()), info

    def check_nodes_usage_002(self, _params):
        """NU-002: Check RAM usage of each node (min/avg/max/dev).

        Call '/v1/nodes/stats' and calculates min/avg/max/dev of 'total_memory' - 'free_memory' (used memory).
        It compares them to RL recommended values, i.e. maximum of 2/3.

        Remedy: Increase RAM on nodes.

        :param _params: None
        :returns: result
        """
        info = {}
        results = {}
        rex = True  # Remote Executor called in subroutine
        quorum_onlys = self._get_quorum_only_nodes()

        for stats in self.api().get('nodes/stats'):
            minimum, average, maximum, std_dev = calc_usage(stats['intervals'], 'free_memory')
            total_mem = self.api().get_value(f'nodes/{stats["uid"]}', 'total_memory')

            node_name = f'node:{stats["uid"]}'
            if stats['uid'] in quorum_onlys:
                node_name += ' (quorum only)'

            results[node_name] = minimum > (total_mem * 1/3)
            info[node_name] = '{}/{}/{}/{} GB ({}/{}/{}/{} %)'.format(to_gb(total_mem - maximum),
                                                                      to_gb(total_mem - average),
                                                                      to_gb(total_mem - minimum),
                                                                      to_gb(std_dev),
                                                                      to_percent((100 / total_mem) * (total_mem - maximum)),
                                                                      to_percent((100 / total_mem) * (total_mem - average)),
                                                                      to_percent((100 / total_mem) * (total_mem - minimum)),
                                                                      to_percent((100 / total_mem) * std_dev))

        return all(results.values()), info

    def check_nodes_usage_003(self, _params):
        """NU-003: Get ephemeral storage usage of each node (min/avg/max/dev).

        Calls '/v1/nodes/stats' and calculates
        min/avg/max/dev of 'ephemeral_storage_size' - 'ephemeral_storage_avail' (used ephemeral storage).

        :param _params: None
        :returns: result
        """
        info = {}
        rex = True  # Remote Executor called in subroutine
        quorum_onlys = self._get_quorum_only_nodes()

        for stats in self.api().get('nodes/stats'):
            minimum, average, maximum, std_dev = calc_usage(stats['intervals'], 'ephemeral_storage_avail')
            total_size = self.api().get_value(f'nodes/{stats["uid"]}', 'ephemeral_storage_size')

            node_name = f'node:{stats["uid"]}'
            if stats['uid'] in quorum_onlys:
                node_name += ' (quorum only)'

            info[node_name] = '{}/{}/{}/{} GB ({}/{}/{}/{} %)'.format(to_gb(total_size - maximum),
                                                                      to_gb(total_size - average),
                                                                      to_gb(total_size - minimum),
                                                                      to_gb(std_dev),
                                                                      to_percent((100 / total_size) * (total_size - maximum)),
                                                                      to_percent((100 / total_size) * (total_size - average)),
                                                                      to_percent((100 / total_size) * (total_size - minimum)),
                                                                      to_percent((100 / total_size) * std_dev))

        return None, info

    def check_nodes_usage_004(self, _params):
        """NU-004: Get persistent storage usage of each node (min/avg/max/dev).

        Calls '/v1/nodes/stats' and calculates
        min/avg/max/dev of 'persistent_storage_size' - 'persistent_storage_avail' (used persistent storage).

        :param _params: None
        :returns: result
        """
        info = {}
        rex = True  # Remote Executor called in subroutine
        quorum_onlys = self._get_quorum_only_nodes()

        for stats in self.api().get('nodes/stats'):
            minimum, average, maximum, std_dev = calc_usage(stats['intervals'], 'persistent_storage_avail')
            total_size = self.api().get_value(f'nodes/{stats["uid"]}', 'persistent_storage_size')

            node_name = f'node:{stats["uid"]}'
            if stats['uid'] in quorum_onlys:
                node_name += ' (quorum only)'

            info[node_name] = '{}/{}/{}/{} GB ({}/{}/{}/{} %)'.format(to_gb(total_size - maximum),
                                                                      to_gb(total_size - average),
                                                                      to_gb(total_size - minimum),
                                                                      to_gb(std_dev),
                                                                      to_percent((100 / total_size) * (total_size - maximum)),
                                                                      to_percent((100 / total_size) * (total_size - average)),
                                                                      to_percent((100 / total_size) * (total_size - minimum)),
                                                                      to_percent((100 / total_size) * std_dev))

        return None, info

    def check_nodes_usage_005(self, _params):
        """NU-005: Get network traffic usage of each node (min/avg/max/dev).

        Calls '/v1/nodes/stats' and calculates min/avg/max/dev of 'ingress_bytes' and 'egress_bytes'.

        :param _params: None
        :returns: result
        """
        info = {}
        rex = True  # Remote Executor called in subroutine
        quorum_onlys = self._get_quorum_only_nodes()

        for stats in self.api().get('nodes/stats'):
            node_name = f'node:{stats["uid"]}'
            if stats['uid'] in quorum_onlys:
                node_name += ' (quorum only)'

            minimum, average, maximum, std_dev = calc_usage(stats['intervals'], 'ingress_bytes')
            info[node_name] = {
                'ingress': '{}/{}/{}/{} GB/s'.format(to_gb(minimum), to_gb(average), to_gb(maximum), to_gb(std_dev)),
            }
            minimum, average, maximum, std_dev = calc_usage(stats['intervals'], 'egress_bytes')
            info[node_name]['egress'] = '{}/{}/{}/{} GB/s'.format(to_gb(minimum), to_gb(average), to_gb(maximum), to_gb(std_dev))

        return None, info
