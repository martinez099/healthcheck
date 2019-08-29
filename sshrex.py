import logging

from subprocess import Popen, PIPE


def exec_ssh(_user, _host, _keyfile, _cmd):
    """
    Execute a remote shell command.

    :param _user: The remote username.
    :param _host: The remote machine.
    :param _keyfile: The private keyfile.
    :param _cmd: The command to execute.
    :return: The response.
    :raise Exception: If an error occurred.
    """
    exec_cmd(' '.join(['ssh', '-i {}'.format(_keyfile), '{}@{}'.format(_user, _host), '-C', _cmd]))


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
