import logging
import os

from subprocess import Popen, PIPE

SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_HOSTNAMES = os.getenv('SSH_HOSTNAMES')
SSH_KEYFILE = os.getenv('SSH_KEYFILE')


def exec_on_all_nodes(_cmd):
    hostnames = eval(SSH_HOSTNAMES)
    return [exec_on_ip(_cmd, hostname) for hostname in hostnames]


def exec_on_ip(_cmd, _ip):
    """
    Execute a ssh remote command.

    :param _cmd: The command to execute.
    :param _ip: The IP address of the remote machine.
    :return: The reponse.
    :raise Exception: If an error occurred.
    """
    return exec_ssh(SSH_USERNAME, _ip, SSH_KEYFILE, _cmd)


def exec_on_node(_cmd, _node_nr):
    """
    Execute a ssh remote command an a configured node.

    :param _cmd: The command to execute.
    :param _node_nr: The index in the array of the configured IP addresses.
    :return: The response.
    :raise Excpetion: If an error occurred.
    """
    hostname = eval(SSH_HOSTNAMES)[_node_nr]
    return exec_ssh(SSH_USERNAME, hostname, SSH_KEYFILE, _cmd)


def exec_ssh(_user, _host, _keyfile, _cmd):
    """
    Execute a remote command.

    :param _user: The remote username.
    :param _host: The remote machine.
    :param _keyfile: The private keyfile.
    :param _cmd: The command to execute.
    :return: The response.
    :raise Exception: If an error occurred.
    """
    return exec_cmd(' '.join(['ssh', '-i {}'.format(_keyfile), '{}@{}'.format(_user, _host), '-C', _cmd]))


def exec_cmd(_args):
    """
    Execute a shell command.

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
        raise Exception(rsp)
