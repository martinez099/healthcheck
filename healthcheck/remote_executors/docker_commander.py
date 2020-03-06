class DockerCommander(object):
    """
    Docker-Commander class.
    """
    def build_cmd(self, _container, _cmd):
        """
        Build a Docker command.

        :param _container: The container name/ID.
        :param _cmd: The command to execute.
        :return: The response.
        :raise Exception: If an error occurred.
        """
        parts = ['docker', 'exec', '--user', 'root', _container, _cmd]
        return ' '.join(parts)
