from concurrent.futures import ThreadPoolExecutor, wait

from healthcheck.common import exec_cmd


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

    def exec_on_ip(self, _cmd, _ip):
        """
        Execute a SSH command on an IP address.

        :param _cmd: The command to execute.
        :param _ip: The IP address of the remote machine.
        :return: The result.
        :raise Exception: If an error occurred.
        """
        return self._exec(self.username, _ip, self.keyfile, _cmd)

    def exec_on_all_ips(self, _cmd):
        """
        Execute a SSH command on all IP addresses.

        :param _cmd: The command to execute.
        :return: The results.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(self.hostnames)) as e:
            futures = [e.submit(self.exec_on_ip, _cmd, ip) for ip in self.hostnames]
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
        return self._exec(self.username, self.hostnames[_node_nr], self.keyfile, _cmd)

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

    def _exec(self, _user, _host, _keyfile, _cmd):
        """
        Execute a SSH command.

        :param _user: The remote username.
        :param _host: The remote machine.
        :param _keyfile: The private keyfile.
        :param _cmd: The command to execute.
        :return: The response.
        :raise Exception: If an error occurred.
        """

        # lookup from cache
        if _host in self.cache and _cmd in self.cache[_host]:
            return self.cache[_host]

        # execute command
        cmd = ' '.join(['ssh', '-i {}'.format(_keyfile), '{}@{}'.format(_user, _host), '-C', _cmd])
        rsp = exec_cmd(cmd)

        # put into cache
        if _host not in self.cache:
            self.cache[_host] = {}
        self.cache[_host][cmd] = rsp

        return rsp
