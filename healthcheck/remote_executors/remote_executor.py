from concurrent.futures import ThreadPoolExecutor, wait
from threading import Lock

from healthcheck.common_funcs import exec_cmd
from healthcheck.remote_executors.ssh_commander import SshCommander
from healthcheck.remote_executors.docker_commander import DockerCommander
from healthcheck.printer_funcs import print_msg, print_success, print_error


class RemoteExecutor(object):
    """
    Remote Executor class.
    """
    _instance = None

    def __init__(self, _commander, _targets):
        """
        :param _commander: A command executor.
        :param _targets: A string containing CSV with targets.
        """
        self.com = _commander
        self.targets = list(map(lambda x: x.strip(), _targets.split(',')))
        self.addrs = {}
        self.locks = {}
        self.cache = {}
        self.connected = False

    @classmethod
    def instance(cls, _config):
        """
        Get singleton instance.

        :param _config: A dict with configuration values.
        :return: The RemoteExecutor singleton.
        """
        if not cls._instance:
            if 'ssh' in _config:
                commander = SshCommander(_config['ssh']['user'], _config['ssh']['key'])
                cls._instance = RemoteExecutor(commander, _config['ssh']['hosts'])
            elif 'docker' in _config:
                commander = DockerCommander()
                cls._instance = RemoteExecutor(commander, _config['docker']['containers'])
            else:
                raise Exception('could not find a valid commander configuration')

        return cls._instance

    def check_connection(self):
        """
        Check SSH connection.
        """
        if not self.connected:
            print_msg('checking SSH connections ...')
            for target in self.targets:
                try:
                    self.exec_uni('sudo pwd', target)
                    print_success(f'- successfully connected to {target}')
                    self.connected = True
                except Exception as e:
                    print_error(f'could not connect to host {target}:', e)
            print_msg('')

    def get_addr(self, _hostname):
        """
        Get internal address of node.

        :param _hostname: The hostname of the node.
        :return: The internal address.
        """
        addrs = self.get_addrs()

        return addrs[_hostname]

    def get_addrs(self):
        """
        Get internal addresses of each node.

        :return: The internal addresses.
        """
        if not self.addrs:
            self.addrs = {future.target: future.result().split()[0] for future in self.exec_broad('hostname -i')}

        return self.addrs

    def get_targets(self):
        """
        Get targets.

        :return: A list of targets.
        """
        return self.targets

    def exec_uni(self, _cmd, _target):
        """
        Execute a remote command.

        :param _cmd: The command to execute.
        :param _target: The remote machine.
        :return: The result.
        :raise Exception: If an error occurred.
        """
        return self._exec(_cmd, _target)

    def exec_multi(self, _cmd_targets):
        """
        Execute multiple remote commands.

        :param _cmd_targets: A list of (command, target).
        :return: The result.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(_cmd_targets)) as e:
            futures = []
            for cmd, target in _cmd_targets:
                future = e.submit(self._exec, cmd, target)
                future.target = target
                future.cmd = cmd
                futures.append(future)
            done, undone = wait(futures)
            assert not undone
            return done

    def exec_broad(self, _cmd):
        """
        Execute a remote command on all targets.

        :param _cmd: The command to execute.
        :return: The results.
        :raise Exception: If an error occurred.
        """
        with ThreadPoolExecutor(max_workers=len(self.targets)) as e:
            futures = []
            for target in self.targets:
                future = e.submit(self.exec_uni, _cmd, target)
                future.target = target
                future.cmd = _cmd
                futures.append(future)
            done, undone = wait(futures)
            assert not undone
            return done

    def _exec(self, _cmd, _target):
        """
        Execute a remote command.

        :param _cmd: The command to execute.
        :param _target: The remote machine.
        :return: The response.
        :raise Exception: If an error occurred.
        """

        # lookup from cache
        if _target in self.cache and _cmd in self.cache[_target]:
            return self.cache[_target][_cmd]

        # build command
        cmd = self.com.build_cmd(_target, _cmd)

        # create lock if not existent
        if _target not in self.locks:
            self.locks[_target] = Lock()

        # execute command
        with self.locks[_target]:
            rsp = exec_cmd(cmd)

        # put into cache
        if _target not in self.cache:
            self.cache[_target] = {}
        self.cache[_target][_cmd] = rsp

        return rsp
