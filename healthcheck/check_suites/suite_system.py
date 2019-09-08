import re

from healthcheck.check_suites.base_suite import BaseCheckSuite


class SystemChecks(BaseCheckSuite):
    """Check System Health"""

    def check_os_version(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_values('nodes')
        os_versions = self.api.get_values('nodes', 'os_version')

        kwargs = {f'node{i + 1}': os_versions[i] for i in range(0, number_of_nodes)}
        return "get os version of all nodes", None, kwargs

    def check_log_file_path(self, *_args, **_kwargs):
        number_of_nodes = self.api.get_number_of_values('nodes')
        rsps = self.ssh.exec_on_all_nodes('df -h /var/opt/redislabs/log')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.split('\n')[1], re.DOTALL) for rsp in rsps]
        log_file_paths = [match.group(1) for match in matches]

        result = any(['/dev/root' not in log_file_path for log_file_path in log_file_paths])
        kwargs = {f'node{i + 1}': log_file_paths[i] for i in range(0, number_of_nodes)}
        return "check if log file path is on root filesystem", result, kwargs

    def check_tmp_file_path(self, *_args, **_kwargs):
        rsps = self.ssh.exec_on_all_nodes('df -h /tmp')
        matches = [re.match(r'^([\w+/]+)\s+.*$', rsp.split('\n')[1], re.DOTALL) for rsp in rsps]
        tmp_file_paths = [match.group(1) for match in matches]

        number_of_nodes = self.api.get_number_of_values('nodes')
        result = any(['/dev/root' not in tmp_file_path for tmp_file_path in tmp_file_paths])
        kwargs = {f'node{i + 1}': tmp_file_paths[i] for i in range(0, number_of_nodes)}
        return "check if tmp file path is on root filesystem", result, kwargs

    def check_swappiness(self, *_args, **_kwargs):
        swappinesses = self.ssh.exec_on_all_nodes('grep swap /etc/sysctl.conf || echo -n inactive')

        number_of_nodes = self.api.get_number_of_values('nodes')
        kwargs = {f'node{i + 1}': swappinesses[i] for i in range(0, number_of_nodes)}
        return "get swap setting of all nodes", None, kwargs

    def check_transparent_hugepage(self, *_args, **_kwargs):
        transparent_hugepages = self.ssh.exec_on_all_nodes('cat /sys/kernel/mm/transparent_hugepage/enabled')

        number_of_nodes = self.api.get_number_of_values('nodes')
        kwargs = {f'node{i + 1}': transparent_hugepages[i] for i in range(0, number_of_nodes)}
        return "get THP setting of all nodes", None, kwargs

    def check_rladmin_status(self, *_args, **_kwargs):
        rsp = self.ssh.exec_on_node('sudo /opt/redislabs/bin/rladmin status', 0)
        found = re.findall(r'^((?!OK).)*$', rsp.strip(), re.MULTILINE)
        not_ok = len(found)

        return "check rladmin status", not not_ok, {'not OK': not_ok}

    def check_rlcheck_result(self, *_args, **_kwargs):
        rsps = self.ssh.exec_on_all_nodes('/opt/redislabs/bin/rlcheck')
        founds = [re.findall(r'^((?!error).)*$', rsp.strip(), re.MULTILINE) for rsp in rsps]
        errors = sum([len(found) for found in founds])

        return "check rlcheck status", not errors, {'rlcheck errors': errors}

    def check_cnm_ctl_status(self, *_args, **_kwargs):
        rsps = self.ssh.exec_on_all_nodes('sudo /opt/redislabs/bin/cnm_ctl status')
        founds = [re.findall(r'^((?!RUNNING).)*$', rsp.strip(), re.MULTILINE) for rsp in rsps]
        not_running = sum([len(found) for found in founds])

        return "check cnm_ctl status", not_running == 0, {'not RUNNING': not_running}

    def check_supervisorctl_status(self, *_args, **_kwargs):
        rsps = self.ssh.exec_on_all_nodes('sudo /opt/redislabs/bin/supervisorctl status')
        founds = [re.findall(r'^((?!RUNNING).)*$', rsp.strip(), re.MULTILINE) for rsp in rsps]
        not_running = sum([len(found) for found in founds])

        return "check supervisorctl status", not not_running, {'not RUNNING': not_running}

    def check_errors_in_syslog(self, *_args, **_kwargs):
        errors = self.ssh.exec_on_all_nodes( 'sudo grep error /var/log/syslog || echo ""')
        found = sum([len(error.strip()) for error in errors])

        return "check errors in syslog", not found, {'syslog errors': found}

    def check_errors_in_install_log(self, *_args, **_kwargs):
        errors = self.ssh.exec_on_all_nodes('grep error /var/opt/redislabs/log/install.log || echo ""')
        found = sum([len(error.strip()) for error in errors])

        return "check errors in install.log", not found, {'install.log errors': found}
