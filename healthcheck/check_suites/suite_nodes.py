import re

from healthcheck.check_suites.base_suite import BaseCheckSuite
from healthcheck.common import exec_cmd


class NodeChecks(BaseCheckSuite):
    """Check Nodes via SSH"""

    def check_hostnames(self, *_args, **_kwargs):
        """check configured hosts"""
        nodes = self.api.get('nodes')
        rsps = self.ssh.exec_on_all_hosts('hostname -I')
        uid_addrs = [(node['uid'], node['addr']) for node in nodes]
        uid_addrs.sort(key=lambda x: x[0])

        result = all([rsp.result() == uid_addrs[1] for rsp, uid_addrs in zip(rsps, uid_addrs)])
        kwargs = {'node:{}'.format(uid): address for uid, address in uid_addrs}
        return result, kwargs

    def check_hosts(self, *_args, **_kwargs):
        """check host reachabilities"""
        results = [exec_cmd(f'ping -c 1 {hostname} > /dev/null && echo $?') for hostname in self.ssh.hostnames]

        kwargs = {hostname: result == '0' for hostname, result in zip(self.ssh.hostnames, results)}
        return True, {'hostnames': kwargs}

    def check_master_node(self, *_args, **_kwargs):
        """get master node"""
        rsp = self.ssh.exec_on_host('sudo /opt/redislabs/bin/rladmin status', self.ssh.hostnames[0])
        found = re.search(r'(node:\d+ master.*)', rsp)
        hostname = re.split(r'\s+', found.group(1))[4]
        ip_address = re.split(r'\s+', found.group(1))[3]

        return True, {'hostname': hostname, 'IP address': ip_address}

    def check_quorum_only(self, *_args, **_kwargs):
        """get quorumg only nodes"""
        node_ids = self.api.get_values('nodes', 'uid')
        rsps = [self.ssh.exec_on_host(f'sudo /opt/redislabs/bin/rladmin info node {uid}', self.ssh.hostnames[0]) for uid in node_ids]
        matches = [re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL) for rsp in rsps]
        quorums = [match.group(1) for match in matches]

        kwargs = {rsp.ip: quorum for rsp, quorum in zip(rsps, quorums)}
        return None, kwargs

    def check_os_version(self, *_args, **_kwargs):
        """get os version"""
        rsps = self.ssh.exec_on_all_hosts('cat /etc/os-release | grep PRETTY_NAME')
        matches = [re.match(r'^PRETTY_NAME="(.*)"$', rsp.result()) for rsp in rsps]
        os_versions = [match.group(1) for match in matches]

        kwargs = {rsp.ip: os_version for rsp, os_version in zip(rsps, os_versions)}
        return None, kwargs

    def check_software_version(self, *_args, **_kwargs):
        """get software version of all nodes"""
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

    def check_transparent_hugepage(self, *_args, **_kwargs):
        """get THP setting of all nodes"""
        rsps = self.ssh.exec_on_all_hosts('cat /sys/kernel/mm/transparent_hugepage/enabled')
        transparent_hugepages = [rsp.result() for rsp in rsps]

        result = all(transparent_hugepage == 'always madvise [never]' for transparent_hugepage in transparent_hugepages)
        kwargs = {rsp.ip: transparent_hugepage for rsp, transparent_hugepage in zip(rsps, transparent_hugepages)}
        return result, kwargs

    def check_rladmin_status(self, *_args, **_kwargs):
        """check rladmin status"""
        rsp = self.ssh.exec_on_host('sudo /opt/redislabs/bin/rladmin status | grep -v endpoint | grep node', self.ssh.hostnames[0])
        found = re.findall(r'^((?!OK).)*$', rsp, re.MULTILINE)
        not_ok = len(found)

        return not not_ok, {'not OK': not_ok}

    def check_rlcheck_result(self, *_args, **_kwargs):
        """check rlcheck status"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/rlcheck')
        founds = [re.findall(r'FAILED', rsp.result().strip(), re.MULTILINE) for rsp in rsps]
        errors = sum([len(found) for found in founds])

        return not errors, {'rlcheck errors': errors}

    def check_cnm_ctl_status(self, *_args, **_kwargs):
        """check cnm_ctl status"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/cnm_ctl status')
        founds = [re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE) for rsp in rsps]
        not_running = sum([len(found) for found in founds])

        return not_running == 0, {'not RUNNING': not_running}

    def check_supervisorctl_status(self, *_args, **_kwargs):
        """check supervisorctl status"""
        rsps = self.ssh.exec_on_all_hosts('sudo /opt/redislabs/bin/supervisorctl status')
        founds = [re.findall(r'^((?!RUNNING).)*$', rsp.result(), re.MULTILINE) for rsp in rsps]
        not_running = sum([len(found) for found in founds])

        return not_running == 1 * len(rsps), {'not RUNNING': not_running}

    def check_errors_in_syslog(self, *_args, **_kwargs):
        """check errors in syslog"""
        rsps = self.ssh.exec_on_all_hosts('sudo grep error /var/log/syslog || echo ""')
        found = sum([len(rsp.result()) for rsp in rsps])

        return not found, {'syslog errors': found}

    def check_errors_in_install_log(self, *_args, **_kwargs):
        """check errors in install.log"""
        rsps = self.ssh.exec_on_all_hosts('grep error /var/opt/redislabs/log/install.log || echo ""')
        found = sum([len(rsp.result()) for rsp in rsps])

        return not found, {'install.log errors': found}

    def check_network_speed(self, *_args, **_kwargs):
        """check network link"""
        results = {}
        for source in self.ssh.hostnames:
            for target in self.ssh.hostnames:
                if source == target:
                    continue
                result = self.ssh.exec_on_host(f'ping -c 3 {target}', source)
                lines = result.split('\n')
                results[f'{source} -> {target}'] = lines[-1:][0]

        return None, results
