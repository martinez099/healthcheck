import re

from healthcheck.check_suites.base_suite import BaseCheckSuite


class NodeChecks(BaseCheckSuite):
    """Check nodes"""

    def _check_connectivity(self):
        self._check_api_connectivity()
        self._check_ssh_connectivity()

    def check_private_ip(self, *_args, **_kwargs):
        """check private IP address"""
        nodes = self.api.get('nodes')
        rsps = self.ssh.exec_on_all_hosts('hostname -I')
        uid_addrs = [(node['uid'], node['addr']) for node in nodes]

        result = all(rsp.result() in map(lambda x: x[1], uid_addrs) for rsp in rsps)
        kwargs = {'node:{}'.format(uid): address for uid, address in uid_addrs}
        return result, kwargs

    def check_master_node(self, *_args, **_kwargs):
        """get master node"""
        rsp = self.ssh.exec_on_host('sudo /opt/redislabs/bin/rladmin status', self.ssh.hostnames[0])
        found = re.search(r'(node:\d+ master.*)', rsp)
        hostname = re.split(r'\s+', found.group(1))[4]
        ip_address = re.split(r'\s+', found.group(1))[3]

        return None, {'hostname': hostname, 'IP address': ip_address}

    def check_os_version(self, *_args, **_kwargs):
        """get os version"""
        rsps = self.ssh.exec_on_all_hosts('cat /etc/os-release | grep PRETTY_NAME')
        matches = [re.match(r'^PRETTY_NAME="(.*)"$', rsp.result()) for rsp in rsps]
        os_versions = [match.group(1) for match in matches]

        kwargs = {rsp.ip: os_version for rsp, os_version in zip(rsps, os_versions)}
        return None, kwargs

    def check_software_version(self, *_args, **_kwargs):
        """get software version"""
        node_ids = self.api.get_values('nodes', 'uid')
        software_versions = self.api.get_values('nodes', 'software_version')

        kwargs = {f'node:{node_id}': software_version for node_id, software_version in zip(node_ids, software_versions)}
        return None, kwargs

    def check_log_file_path(self, *_args, **_kwargs):
        """check if log file path is NOT on root filesystem"""
        rsps = self.ssh.exec_on_all_hosts('df -h /var/opt/redislabs/log')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        log_file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in log_file_path for log_file_path in log_file_paths])
        kwargs = {rsp.ip: log_file_path for rsp, log_file_path in zip(rsps, log_file_paths)}
        return result, kwargs

    def check_tmp_file_path(self, *_args, **_kwargs):
        """check if tmp file path is NOT on root filesystem"""
        rsps = self.ssh.exec_on_all_hosts('df -h /tmp')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.result().split('\n')[1], re.DOTALL) for rsp in rsps]
        tmp_file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in tmp_file_paths])
        kwargs = {rsp.ip: tmp_file_path for rsp, tmp_file_path in zip(rsps, tmp_file_paths)}
        return result, kwargs

    def check_swappiness(self, *_args, **_kwargs):
        """check swap setting"""
        rsps = self.ssh.exec_on_all_hosts('grep swap /etc/sysctl.conf || echo inactive')
        swappinesses = [rsp.result() for rsp in rsps]

        result = any([swappiness == 'inactive' for swappiness in swappinesses])
        kwargs = {rsp.ip: swappiness for rsp, swappiness in zip(rsps, swappinesses)}
        return result, kwargs

    def check_transparent_hugepages(self, *_args, **_kwargs):
        """check THP setting"""
        rsps = self.ssh.exec_on_all_hosts('cat /sys/kernel/mm/transparent_hugepage/enabled')
        transparent_hugepages = [rsp.result() for rsp in rsps]

        result = all(transparent_hugepage == 'always madvise [never]' for transparent_hugepage in transparent_hugepages)
        kwargs = {rsp.ip: transparent_hugepage for rsp, transparent_hugepage in zip(rsps, transparent_hugepages)}
        return result, kwargs

    def check_rladmin_status(self, *_args, **_kwargs):
        """check rladmin status"""
        rsp = self.ssh.exec_on_host('sudo /opt/redislabs/bin/rladmin status | grep -v endpoint | grep node', self.ssh.hostnames[0])
        not_ok = re.findall(r'^((?!OK).)*$', rsp, re.MULTILINE)

        return len(not_ok) == 0, {'not OK': len(not_ok)} if not_ok else {'OK': 'all'}

    def check_rlcheck_result(self, *_args, **_kwargs):
        """check rlcheck status"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/rlcheck')
        failed = [(re.findall(r'FAILED', rsp.result().strip(), re.MULTILINE), rsp.ip) for rsp in rsps]
        errors = sum([len(f[0]) for f in failed])

        return not errors, {f[1]: len(f[0]) == 0 for f in failed}

    def check_cnm_ctl_status(self, *_args, **_kwargs):
        """check cnm_ctl status"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/cnm_ctl status')
        running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.ip) for rsp in rsps]
        not_running = sum([len(r[0]) for r in running])

        return not_running == 0,  {r[1]: len(r[0]) == 0 for r in running}

    def check_supervisorctl_status(self, *_args, **_kwargs):
        """check supervisorctl status"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/supervisorctl status')
        running = [(re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE), rsp.ip) for rsp in rsps]
        not_running = sum([len(r[0]) for r in running])

        return not_running == 1 * len(rsps), {r[1]: len(r[0]) == 1 for r in running}

    def check_errors_in_install_log(self, *_args, **_kwargs):
        """check errors in install.log"""
        rsps = self.ssh.exec_on_all_hosts('grep error /var/opt/redislabs/log/install.log || echo ""')
        errors = sum([len(rsp.result()) for rsp in rsps])

        return not errors, {rsp.ip: len(rsp.result()) for rsp in rsps}

    def check_network_link(self, *_args, **_kwargs):
        """get network link"""
        cmd_ips = []
        for source in self.ssh.hostnames:
            for target in self.ssh.hostnames:
                if source == target:
                    continue
                cmd_ips.append((f'ping -c 4 {target}', source))

        results = self.ssh.exec_on_hosts(cmd_ips)
        kwargs = {}
        for result in results:
            lines = result.result().split('\n')
            kwargs[f'{result.ip}'] = lines[-1:][0]

        return None, kwargs

    def check_shards_balance(self, *_args, **_kwargs):
        """check if shards are balanced accross nodes"""
        nodes = self.api.get('nodes')
        shards_per_node = {f'node:{node["uid"]}': node['shard_count'] for node in nodes}

        # remove quorum-only nodes
        rsps = [self.ssh.exec_on_host(f'sudo /opt/redislabs/bin/rladmin info node {node["uid"]}',
                                      self.ssh.hostnames[0]) for node in nodes]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorum_onlys = {f'node:{node["uid"]}': match.group(1) == 'enabled' for node, match in zip(nodes, matches)}
        for node, shards in set(shards_per_node.items()):
            if quorum_onlys[node]:
                del shards_per_node[node]

        balanced = max(shards_per_node.values()) - min(shards_per_node.values()) <= 1
        return balanced, shards_per_node
