class SshCommander(object):
    """
    SSH-Commander class.
    """
    def __init__(self, _username=None, _keyfile=None):
        """
        :param _username: The ssh username to log in.
        :param _keyfile: The path to the ssh identity file.
        """
        self.username = _username
        self.keyfile = _keyfile

    def build_cmd(self, _host, _cmd):
        """
        Build a SSH command.

        :param _host: The remote machine.
        :param _cmd: The command to execute.
        :return: The response.
        :raise Exception: If an error occurred.
        """
        parts = ['ssh']
        if self.keyfile:
            parts.append('-i {}'.format(self.keyfile))
        if self.username:
            parts.append('{}@{}'.format(self.username, _host))
        else:
            parts.append(_host)
        parts.append('''-C '{}' '''.format(_cmd))
        return ' '.join(parts)
