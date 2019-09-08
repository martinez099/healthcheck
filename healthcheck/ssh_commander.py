import logging

from concurrent.futures import ThreadPoolExecutor, wait
from subprocess import Popen, PIPE


class SshCommander(object):
    """
    SSH-Commander class.
    """

    def __init__(self, _username, _hostnames, _keyfile):
        """
        :param _username: The ssh username to log in.
        :param _hostnames: A list with hostnames to log in.
        :param _keyfile: The path to the ssh identity file.
        """
        self.username = _username
        self.hostnames = _hostnames
        self.keyfile = _keyfile
        self.cache = {}

    def _exec_on_ip(self, _cmd, _ip):
        """
        Execute a SSH command on an IP address.

        :param _cmd: The command to execute.
        :param _ip: The IP address of the remote machine.
        :return: The result.
        :raise Exception: If an error occurred.
        """
        return self._exec_ssh(self.username, _ip, self.keyfile, _cmd)

    def _exec_on_all_ips(self, _cmd):
        """
        Execute a SSH command on all IP addresses.

        :param _cmd: The command to execute.
        :return: The results.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(self.hostnames)) as e:
            futures = [e.submit(self._exec_on_ip, _cmd, ip) for ip in self.hostnames]
            done, undone = wait(futures)
            assert not undone
            return [d.result() for d in done]

    def exec_on_node(self, _cmd, _node_nr):
        """
        Execute a SSH remote command an a node.

        :param _cmd: The command to execute.
        :param _node_nr: The index in the array of the configured IP addresses.
        :return: The response.
        :raise Excpetion: If an error occurred.
        """
        return self._exec_ssh(self.username, self.hostnames[_node_nr], self.keyfile, _cmd)

    def exec_on_all_nodes(self, _cmd):
        """
        Execute a SSH command on all nodes.

        :param _cmd: The command to execute.
        :return: The results.
        :raise Excpetion: If an error occurred.
        """
        number_of_nodes = len(self.hostnames)
        with ThreadPoolExecutor(max_workers=number_of_nodes) as e:
            futures = [e.submit(self.exec_on_node, _cmd, node_nr) for node_nr in range(0, number_of_nodes)]
            done, undone = wait(futures)
            assert not undone
            return [d.result() for d in done]

    def _exec_ssh(self, _user, _host, _keyfile, _cmd):
        """
        Execute a SSH command.

        :param _user: The remote username.
        :param _host: The remote machine.
        :param _keyfile: The private keyfile.
        :param _cmd: The command to execute.
        :return: The response.
        :raise Exception: If an error occurred.
        """
        if _host in self.cache and _cmd in self.cache[_host]:
            return self.cache[_host]

        cmd = ' '.join(['ssh', '-i {}'.format(_keyfile), '{}@{}'.format(_user, _host), '-C', _cmd])
        rsp = SshCommander.exec_cmd(cmd)
        if _host not in self.cache:
            self.cache[_host] = {}
        self.cache[_host][cmd] = rsp

        return rsp

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
