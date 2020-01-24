from threading import Lock
from concurrent.futures import ThreadPoolExecutor, wait

from healthcheck.common_funcs import exec_cmd
from healthcheck.render_engine import print_msg, print_success, print_error


class SshCommander(object):
    """
    SSH-Commander class.
    """
    _instance = None

    def __init__(self, _hostnames,  _username=None, _keyfile=None):
        """
        :param _hostnames: A string containing CSV with hostnames to log in.
        :param _username: The ssh username to log in.
        :param _keyfile: The path to the ssh identity file.
        """
        self.hostnames = list(map(lambda x: x.strip(), _hostnames.split(',')))
        self.username = _username
        self.keyfile = _keyfile
        self.locks = {}
        self.cache = {}
        self.internal_addrs = {future.hostname: future.result() for future in self.exec_on_all_hosts('hostname -I')}
        self.check_connectivity()

    @classmethod
    def instance(cls, _config):
        """
        Get singleton instance.

        :param _config: A dict with configuration values.
        :return: The SshCommander singleton.
        """
        if not cls._instance:
            cls._instance = SshCommander(_config['ssh']['hosts'], _config['ssh']['user'], _config['ssh']['key'])
        return cls._instance

    def check_connectivity(self):
        """
        Check connection.
        """
        print_msg('checking SSH connection ...')
        for hostname in self.hostnames:
            try:
                self.exec_on_host('sudo -v', hostname)
                print_success(f'- successfully connected to {hostname}')
            except Exception as e:
                print_error(f'could not connect to host {hostname}:', e)
                exit(3)
        print('')

    def get_internal_addr(self, _hostname):
        """
        Get internal address of node.

        :param _hostname: The hostname of the node.
        :return: The internal address.
        """
        return self.internal_addrs[_hostname]

    def get_internal_addrs(self):
        """
        Get internal addresses of all nodes.

        :return: A dict mapping hostname -> internal address.
        """
        return self.internal_addrs

    def exec_on_host(self, _cmd, _hostname):
        """
        Execute a SSH command on a host.

        :param _cmd: The command to execute.
        :param _hostname: The hostname of the remote machine.
        :return: The result.
        :raise Exception: If an error occurred.
        """
        return self._exec(_hostname, _cmd, self.username, self.keyfile)

    def exec_on_hosts(self, _cmd_hostnames):
        """
        Execute multiple SSH commands.

        :param _cmd_hostnames: A list of (command, hostname).
        :return: The result.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(_cmd_hostnames)) as e:
            futures = []
            for cmd, hostname in _cmd_hostnames:
                future = e.submit(self._exec, hostname, cmd, self.username, self.keyfile)
                future.hostname = hostname
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
            for hostname in self.hostnames:
                future = e.submit(self.exec_on_host, _cmd, hostname)
                future.hostname = hostname
                future.cmd = _cmd
                futures.append(future)
            done, undone = wait(futures)
            assert not undone
            return done

    def _exec(self, _host, _cmd, _user=None, _keyfile=None):
        """
        Execute a SSH command.

        :param _host: The remote machine.
        :param _cmd: The command to execute.
        :param _user: The remote username.
        :param _keyfile: The private keyfile.
        :return: The response.
        :raise Exception: If an error occurred.
        """

        # lookup from cache
        if _host in self.cache and _cmd in self.cache[_host]:
            return self.cache[_host]

        # build command
        parts = ['ssh']
        if _keyfile:
            parts.append('-i {}'.format(_keyfile))
        if _user:
            parts.append('{}@{}'.format(_user, _host))
        else:
            parts.append(_host)
        parts.append('''-C '{}' '''.format(_cmd))
        cmd = ' '.join(parts)

        # create lock
        if _host not in self.locks:
            self.locks[_host] = Lock()

        # execute command
        with self.locks[_host]:
            rsp = exec_cmd(cmd)

        # put into cache
        if _host not in self.cache:
            self.cache[_host] = {}
        self.cache[_host][cmd] = rsp

        return rsp
