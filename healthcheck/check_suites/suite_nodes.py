import re

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common_funcs import to_gb, to_percent


class NodeChecks(BaseCheckSuite):
    """Nodes"""

    def _check_connectivity(self):
        self._check_api_connectivity()
        self._check_ssh_connectivity()

    def check_master_node(self, *_args, **_kwargs):
        """get master node"""
        rsp = self.ssh.exec_on_host('sudo /opt/redislabs/bin/rladmin status', self.ssh.hostnames[0])
        found = re.search(r'(^node:\d+\s+master.*$)', rsp, re.MULTILINE)
        hostname = re.split(r'\s+', found.group(1))[4]
        ip_address = re.split(r'\s+', found.group(1))[3]

        return None, {'hostname': hostname, 'IP address': ip_address}

    def check_os_version(self, *_args, **_kwargs):
        """get OS version"""
        rsps = self.ssh.exec_on_all_hosts('cat /etc/os-release | grep PRETTY_NAME')
        matches = [re.match(r'^PRETTY_NAME="(.*)"$', rsp.result()) for rsp in rsps]
        os_versions = [match.group(1) for match in matches]

        kwargs = {rsp.ip: os_version for rsp, os_version in zip(rsps, os_versions)}
        return None, kwargs

    def check_software_version(self, *_args, **_kwargs):
        """get RS version"""
        node_ids = self.api.get_values('nodes', 'uid')
        software_versions = self.api.get_values('nodes', 'software_version')

        kwargs = {f'node:{node_id}': software_version for node_id, software_version in zip(node_ids, software_versions)}
        return None, kwargs

    def check_log_file_path(self, *_args, **_kwargs):
        """check if log file path is not on root filesystem"""
        rsps = self.ssh.exec_on_all_hosts('sudo df -h /var/opt/redislabs/log')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        log_file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in log_file_path for log_file_path in log_file_paths])
        kwargs = {rsp.ip: log_file_path for rsp, log_file_path in zip(rsps, log_file_paths)}
        return result, kwargs

    def check_ephemeral_storage_path(self, *_args, **_kwargs):
        """check if ephemeral storage path is not on root filesystem"""
        storage_paths = self.api.get_values('nodes', 'ephemeral_storage_path')
        rsps = self.ssh.exec_on_all_hosts(f'sudo df -h {storage_paths[0]}')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in file_paths])
        kwargs = {rsp.ip: tmp_file_path for rsp, tmp_file_path in zip(rsps, file_paths)}
        return result, kwargs

    def check_persistent_storage_path(self, *_args, **_kwargs):
        """check if persistent storage path is not on root filesystem"""
        storage_paths = self.api.get_values('nodes', 'persistent_storage_path')
        rsps = self.ssh.exec_on_all_hosts(f'sudo df -h {storage_paths[0]}')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in file_paths])
        kwargs = {rsp.ip: tmp_file_path for rsp, tmp_file_path in zip(rsps, file_paths)}
        return result, kwargs

    def check_swappiness(self, *_args, **_kwargs):
        """check if swappiness is disabled"""
        rsps = self.ssh.exec_on_all_hosts('grep swap /etc/sysctl.conf || echo inactive')
        swappinesses = [rsp.result() for rsp in rsps]

        result = any([swappiness == 'inactive' for swappiness in swappinesses])
        kwargs = {rsp.ip: swappiness for rsp, swappiness in zip(rsps, swappinesses)}
        return result, kwargs

    def check_transparent_hugepages(self, *_args, **_kwargs):
        """check if THP is disabled"""
        rsps = self.ssh.exec_on_all_hosts('cat /sys/kernel/mm/transparent_hugepage/enabled')
        transparent_hugepages = [rsp.result() for rsp in rsps]

        result = all(transparent_hugepage == 'always madvise [never]' for transparent_hugepage in transparent_hugepages)
        kwargs = {rsp.ip: transparent_hugepage for rsp, transparent_hugepage in zip(rsps, transparent_hugepages)}
        return result, kwargs

    def check_rladmin_status(self, *_args, **_kwargs):
        """check if 'rladmin status' has errors"""
        rsp = self.ssh.exec_on_host('sudo /opt/redislabs/bin/rladmin status | grep -v endpoint | grep node', self.ssh.hostnames[0])
        not_ok = re.findall(r'^((?!OK).)*$', rsp, re.MULTILINE)

        return len(not_ok) == 0, {'not OK': len(not_ok)} if not_ok else {'OK': 'all'}

    def check_rlcheck_result(self, *_args, **_kwargs):
        """check if 'rlcheck' has errors"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/rlcheck')
        failed = [(re.findall(r'FAILED', rsp.result().strip(), re.MULTILINE), rsp.ip) for rsp in rsps]
        errors = sum([len(f[0]) for f in failed])

        return not errors, {f[1]: len(f[0]) for f in failed}

    def check_cnm_ctl_status(self, *_args, **_kwargs):
        """check 'cnm_ctl status'"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/cnm_ctl status')
        not_running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.ip) for rsp in rsps]
        sum_not_running = sum([len(r[0]) for r in not_running])

        return sum_not_running == 0,  {r[1]: len(r[0]) for r in not_running}

    def check_supervisorctl_status(self, *_args, **_kwargs):
        """check 'supervisorctl status'"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/supervisorctl status')
        not_running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.ip) for rsp in rsps]
        sum_not_running = sum([len(r[0]) for r in not_running])

        return sum_not_running == 1 * len(rsps), {r[1]: len(r[0]) for r in not_running}

    def check_errors_in_install_log(self, *_args, **_kwargs):
        """check if install.log has errors"""
        rsps = self.ssh.exec_on_all_hosts('grep error /var/opt/redislabs/log/install.log || echo ""')
        errors = sum([len(rsp.result()) for rsp in rsps])

        return not errors, {rsp.ip: len(rsp.result()) for rsp in rsps}

    def check_network_link(self, *_args, **_kwargs):
        """get network link speed"""
        cmd_ips = []
        for source in self.ssh.hostnames:
            for target in self.ssh.hostnames:
                if source == target:
                    continue
                cmd_ips.append((f'ping -c 4 {target}', source))

        # calculate averages
        _min, avg, _max, mdev = .0, .0, .0, .0
        futures = self.ssh.exec_on_hosts(cmd_ips)
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

        kwargs = {key: '{:.3f}/{:.3f}/{:.3f}/{:.3f} ms'.format(_min, avg, _max, mdev)}
        return None, kwargs

    def check_cpu_usage(self):
        """check maximum CPU usage"""
        kwargs = {}
        results = []
        stats = self.api.get('nodes/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']

            min_cpu_idle = min(i['cpu_idle'] for i in filter(lambda x: x.get('cpu_idle'), ints))
            results.append(min_cpu_idle < 0.2)
            kwargs[f'node:{uid}'] = '{}%'.format(to_percent(1 - min_cpu_idle))

        return not any(results), kwargs

    def check_ram_usage(self):
        """check minimum free RAM"""
        kwargs = {}
        results = []
        stats = self.api.get('nodes/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']

            min_free_memory = min(i['free_memory'] for i in filter(lambda x: x.get('free_memory'), ints))
            total_memory = self.api.get(f'nodes/{uid}')['total_memory']
            results.append(min_free_memory < total_memory * (2/3))
            kwargs[f'node:{uid}'] = '{} GB'.format(to_gb(min_free_memory))

        return not any(results), kwargs

    def check_ephemeral_storage_usage(self):
        """get minimum available ephemeral storage"""
        kwargs = {}
        stats = self.api.get('nodes/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']

            min_ephemeral_storage = min(i['ephemeral_storage_avail'] for i in filter(lambda x: x.get('ephemeral_storage_avail'), ints))
            kwargs[f'node:{uid}'] = '{} GB'.format(to_gb(min_ephemeral_storage))

        return None, kwargs

    def check_persistent_storage_usage(self):
        """get minimum available persistent storage"""
        kwargs = {}
        stats = self.api.get('nodes/stats')

        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']

            min_ephemeral_storage = min(i['persistent_storage_avail'] for i in filter(lambda x: x.get('persistent_storage_avail'), ints))
            kwargs[f'node:{uid}'] = '{} GB'.format(to_gb(min_ephemeral_storage))

        return None, kwargs

    def check_cpu_balance(self, *_args, **_kwargs):
        """get average CPU usage"""
        stats = self.api.get('nodes/stats')

        # get quorum-only node
        nodes = self.api.get('nodes')
        rsps = [self.ssh.exec_on_host(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                      self.ssh.hostnames[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = list(map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

        kwargs = {}
        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']

            # calculate average CPU idle
            cpu_idles = list(filter(lambda x: x.get('cpu_idle'), ints))
            sum_cpu_idle = sum(i['cpu_idle'] for i in cpu_idles)
            avg_cpu_idle = sum_cpu_idle/len(cpu_idles)

            node_name = f'node:{uid}'
            if uid in quorum_onlys:
                node_name += ' (quorum only)'
            kwargs[node_name] = '{}%'.format(to_percent(1 - avg_cpu_idle))

        return None, kwargs

    def check_ram_balance(self, *_args, **_kwargs):
        """get average RAM uasge"""
        stats = self.api.get('nodes/stats')

        # get quorum-only node
        nodes = self.api.get('nodes')
        rsps = [self.ssh.exec_on_host(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                      self.ssh.hostnames[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = list(map(lambda x: x[0]['uid'], filter(lambda x: x[1].group(1) == 'enabled', zip(nodes, matches))))

        kwargs = {}
        for i in range(0, len(stats)):
            ints = stats[i]['intervals']
            uid = stats[i]['uid']

            # calculate average available memory
            avail_mems = list(filter(lambda x: x.get('available_memory'), ints))
            sum_avail_mem = sum(i['available_memory'] for i in avail_mems)
            avg_avail_mem = sum_avail_mem/len(avail_mems)

            total_mem = self.api.get_value(f'nodes/{uid}', 'total_memory')

            node_name = f'node:{uid}'
            if uid in quorum_onlys:
                node_name += ' (quorum only)'
            kwargs[node_name] = '{} GB'.format(to_gb(total_mem - avg_avail_mem))

        return None, kwargs
