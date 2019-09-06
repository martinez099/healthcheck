import concurrent.futures
import logging
import re

from subprocess import Popen, PIPE


class SshCommander(object):
    """
    SSH-Commander class.
    """

    def __init__(self, _usernae, _hostnames, _keyfile):
        """
        :param _usernae: The ssh username to log in.
        :param _hostnames: A list with hostnames to log in.
        :param _keyfile: The path to the ssh identity file.
        """
        self.username = _usernae
        self.hostnames = _hostnames
        self.keyfile = _keyfile

    def get_master_node(self):
        cmd = 'sudo /opt/redislabs/bin/rladmin status | grep master'
        rsp = self._exec_on_node(cmd, 0)
        match = re.search(r'(node:\d+ master.*)', rsp)
        return re.split(r'\s+', match.group(1))[4]

    def get_log_file_path(self, _node_nr=0):
        cmd = 'df -h /var/opt/redislabs/log'
        rsp = self._exec_on_node(cmd, _node_nr)
        match = re.match(r'^([\w+/]+)\s+.*$', rsp.split('\n')[1], re.DOTALL)
        return match.group(1)

    def get_log_file_paths(self, _number_of_nodes):
        return SshCommander.exec_func_on_all_nodes(self.get_log_file_path, _number_of_nodes)

    def get_tmp_file_path(self, _node_nr=0):
        cmd = 'df -h /tmp'
        rsp = self._exec_on_node(cmd, _node_nr)
        match = re.match(r'^([\w+/]+)\s+.*$', rsp.split('\n')[1], re.DOTALL)
        return match.group(1)

    def get_tmp_file_paths(self, _number_of_nodes):
        return SshCommander.exec_func_on_all_nodes(self.get_tmp_file_path, _number_of_nodes)

    def get_quorum_only(self, _node_nr=0):
        cmd = f'sudo /opt/redislabs/bin/rladmin info node {_node_nr + 1} | grep quorum || echo not found'
        rsp = self._exec_on_node(cmd, _node_nr)
        match = re.match(r'^.*quorum only: (\w+).*$', rsp, re.DOTALL)
        return match.group(1)

    def get_quorum_onlys(self, _number_of_nodes):
        return SshCommander.exec_func_on_all_nodes(self.get_quorum_only, _number_of_nodes)

    def get_swappiness(self, _node_nr=0):
        cmd = 'grep swap /etc/sysctl.conf || echo -n inactive'
        return self._exec_on_node(cmd, _node_nr)

    def get_swappinesses(self, _number_of_nodes):
        return SshCommander.exec_func_on_all_nodes(self.get_swappiness, _number_of_nodes)

    def get_transparent_hugepage(self, _node_nr=0):
        cmd = 'cat /sys/kernel/mm/transparent_hugepage/enabled'
        return self._exec_on_node(cmd, _node_nr)

    def get_transaprent_hugepages(self, _number_of_nodes):
        return SshCommander.exec_func_on_all_nodes(self.get_transparent_hugepage, _number_of_nodes)

    def run_rladmin_status(self, _node_nr=0):
        cmd = 'sudo /opt/redislabs/bin/rladmin status extra all'
        return self._exec_on_node(cmd, _node_nr)

    def run_rlcheck(self, _node_nr=0):
        cmd = 'sudo /opt/redislabs/bin/rlcheck'
        return self._exec_on_node(cmd, _node_nr)

    def run_cnm_ctl_status(self, _node_nr=0):
        cmd = 'sudo /opt/redislabs/bin/cnm_ctl status'
        return self._exec_on_node(cmd, _node_nr)

    def run_supervisorctl_status(self, _node_nr=0):
        cmd = 'sudo /opt/redislabs/bin/supervisorctl status'
        return self._exec_on_node(cmd, _node_nr)

    def find_errors_in_syslog(self, _node_nr=0):
        cmd = 'sudo grep error /var/log/syslog || echo no error'
        return self._exec_on_node(cmd, _node_nr)

    def find_errors_in_install_log(self, _node_nr=0):
        cmd = 'zgrep error /var/opt/redislabs/log/install.log || echo no error'
        return self._exec_on_node(cmd, _node_nr)

    def _exec_on_ip(self, _cmd, _ip):
        """
        Execute a SSH command on an IP address.

        :param _cmd: The command to execute.
        :param _ip: The IP address of the remote machine.
        :return: The result.
        :raise Exception: If an error occurred.
        """
        return SshCommander.exec_ssh(self.username, _ip, self.keyfile, _cmd)

    def _exec_on_all_ips(self, _cmd):
        """
        Execute a SSH command on all IP addresses.

        :param _cmd: The command to execute.
        :return: The results.
        :raise Exception: If an error occurred.
        """
        number_of_nodes = len(self.hostnames)
        with concurrent.futures.ThreadPoolExecutor(max_workers=number_of_nodes) as e:
            futures = [e.submit(self._exec_on_ip, _cmd, ip) for ip in self.hostnames]
            done, undone = concurrent.futures.wait(futures)
            assert not undone
            return [d.result() for d in done]

    def _exec_on_node(self, _cmd, _node_nr):
        """
        Execute a SSH remote command an a node.

        :param _cmd: The command to execute.
        :param _node_nr: The index in the array of the configured IP addresses.
        :return: The response.
        :raise Excpetion: If an error occurred.
        """
        return SshCommander.exec_ssh(self.username, self.hostnames[_node_nr], self.keyfile, _cmd)

    def _exec_on_all_nodes(self, _cmd, _number_of_nodes):
        """
        Execute a SSH command on all nodes.

        :param _cmd: The command to execute.
        :param _number_of_nodes: The amount of nodes.
        :return: The results.
        :raise Excpetion: If an error occurred.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=_number_of_nodes) as e:
            futures = [e.submit(self._exec_on_node, _cmd, node_nr) for node_nr in range(0, _number_of_nodes)]
            done, undone = concurrent.futures.wait(futures)
            assert not undone
            return [d.result() for d in done]

    @staticmethod
    def exec_ssh(_user, _host, _keyfile, _cmd):
        """
        Execute a SSH command.

        :param _user: The remote username.
        :param _host: The remote machine.
        :param _keyfile: The private keyfile.
        :param _cmd: The command to execute.
        :return: The response.
        :raise Exception: If an error occurred.
        """
        cmd = ' '.join(['ssh', '-i {}'.format(_keyfile), '{}@{}'.format(_user, _host), '-C', _cmd])
        return SshCommander.exec_cmd(cmd)

    @staticmethod
    def exec_cmd(_args):
        """
        Execute a SSH command string.

        :param _args: The command string.
        :return: The response.
        :raise Exception: If an error occurred.
        """
        logging.debug(_args)
        proc = Popen(_args, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
        proc.wait()
        if proc.returncode == 0:
            rsp = proc.stdout.read().decode('utf-8')
            return rsp
        else:
            rsp = proc.stderr.read().decode('utf-8')
            raise Exception(f'error during ssh remote execution (return code {proc.returncode}): {rsp}')

    @staticmethod
    def exec_func_on_all_nodes(_func, _number_of_nodes):
        """
        Execute a function on all noces.

        :param _func: The function to execeute.
        :param _number_of_nodes: The number of nodes.
        :return: The results.
        :raise Exception: If an error occurred.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=_number_of_nodes) as e:
            futures = [e.submit(_func, node_nr) for node_nr in range(0, _number_of_nodes)]
            done, undone = concurrent.futures.wait(futures)
            assert not undone
            return [d.result() for d in done]
