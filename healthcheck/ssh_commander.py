from concurrent.futures import ThreadPoolExecutor, wait

from healthcheck.common import exec_cmd


class SshCommander(object):
    """
    SSH-Commander class.
    """

    def __init__(self, _username, _hostnames, _keyfile):
        """
        :param _username: The ssh username to log in.
        :param _hostnames: A string containing CSV with hostnames to log in.
        :param _keyfile: The path to the ssh identity file.
        """
        self.username = _username
        self.hostnames = list(map(lambda x: x.strip(), _hostnames.split(',')))
        self.keyfile = _keyfile
        self.cache = {}

    def exec_on_host(self, _cmd, _ip):
        """
        Execute a SSH command on a host.

        :param _cmd: The command to execute.
        :param _ip: The IP address of the remote machine.
        :return: The result.
        :raise Exception: If an error occurred.
        """
        return self._exec(self.username, _ip, self.keyfile, _cmd)

    def exec_on_hosts(self, _cmd_ips):
        """
        Execute multiple SSH commands.

        :param _cmd_ips: A list of (commands, IP addresses).
        :return: The result.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(_cmd_ips)) as e:
            futures = []
            for cmd, ip in _cmd_ips:
                future = e.submit(self._exec, self.username, ip, self.keyfile, cmd)
                future.ip = ip
                future.cmd = cmd
                futures.append(future)
            done, undone = wait(futures)
            assert not undone
            return done

    def exec_on_all_hosts(self, _cmd):
        """
        Execute a SSH command on all hosts.

        :param _cmd: The command to execute.
        :return: The results.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(self.hostnames)) as e:
            futures = []
            for ip in self.hostnames:
                future = e.submit(self.exec_on_host, _cmd, ip)
                future.ip = ip
                future.cmd = _cmd
                futures.append(future)
            done, undone = wait(futures)
            assert not undone
            return done

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
