import logging
import os

from subprocess import Popen, PIPE

SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_HOSTNAMES = os.getenv('SSH_HOSTNAMES')
SSH_KEYFILE = os.getenv('SSH_KEYFILE')


def _exec_on_all_nodes(_cmd):
    hostnames = eval(SSH_HOSTNAMES)
    return [_exec_on_ip(_cmd, hostname) for hostname in hostnames]


def _exec_on_ip(_cmd, _ip):
    """
    Execute a ssh remote command.

    :param _cmd: The command to execute.
    :param _ip: The IP address of the remote machine.
    :return: The reponse.
    :raise Exception: If an error occurred.
    """
    return _exec_ssh(SSH_USERNAME, _ip, SSH_KEYFILE, _cmd)


def _exec_on_node(_cmd, _node_nr):
    """
    Execute a ssh remote command an a configured node.

    :param _cmd: The command to execute.
    :param _node_nr: The index in the array of the configured IP addresses.
    :return: The response.
    :raise Excpetion: If an error occurred.
    """
    hostname = eval(SSH_HOSTNAMES)[_node_nr]
    return _exec_ssh(SSH_USERNAME, hostname, SSH_KEYFILE, _cmd)


def _exec_ssh(_user, _host, _keyfile, _cmd):
    """
    Execute a remote command.

    :param _user: The remote username.
    :param _host: The remote machine.
    :param _keyfile: The private keyfile.
    :param _cmd: The command to execute.
    :return: The response.
    :raise Exception: If an error occurred.
    """
    return _exec_cmd(' '.join(['ssh', '-i {}'.format(_keyfile), '{}@{}'.format(_user, _host), '-C', _cmd]))


def _exec_cmd(_args):
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


def get_log_file_paths():
    return _exec_on_all_nodes('df -h /var/opt/redislabs/log')


def get_tmp_file_path():
    return _exec_on_all_nodes('df -h /tmp')


def get_quorums(_nr_of_nodes):
    cmd = 'sudo /opt/redislabs/bin/rladmin info node {} | grep quorum'
    return [_exec_on_node(cmd.format(i), 0) for i in range(1, _nr_of_nodes + 1)]


def get_master_node():
    cmd = 'sudo /opt/redislabs/bin/rladmin status | grep master'
    return _exec_on_node(cmd, 0)


def get_swappiness(_node_nr=0):
    cmd = 'grep swap /etc/sysctl.conf || echo inactive'
    return _exec_on_node(cmd, _node_nr)


def get_transparent_hugepages(_node_nr=0):
    cmd = 'cat /sys/kernel/mm/transparent_hugepage/enabled'
    return _exec_on_node(cmd, _node_nr)


def run_rladmin_status(_node_nr=0):
    cmd = 'sudo /opt/redislabs/bin/rladmin status extra all'
    return _exec_on_node(cmd, _node_nr)


def run_rlcheck(_node_nr=0):
    cmd = 'sudo /opt/redislabs/bin/rlcheck'
    return _exec_on_node(cmd, _node_nr)


def run_cnm_ctl_status(_node_nr=0):
    cmd = 'sudo /opt/redislabs/bin/cnm_ctl status'
    return _exec_on_node(cmd, _node_nr)


def run_supervisorctl_status(_node_nr=0):
    cmd = 'sudo /opt/redislabs/bin/supervisorctl status'
    return _exec_on_node(cmd, _node_nr)


def find_errors_in_syslog(_node_nr=0):
    cmd = 'sudo grep error /var/log/syslog || echo no error'
    return _exec_on_node(cmd, _node_nr)


def find_errors_in_install_log(_node_nr=0):
    cmd = 'zgrep error /var/opt/redislabs/log/install.log || echo no error'
    return _exec_on_node(cmd, _node_nr)


def find_errors_in_logs(_node_nr=0):
    cmd = 'grep error /var/opt/redislabs/log/*.log || echo no error'
    return _exec_on_node(cmd, _node_nr)
