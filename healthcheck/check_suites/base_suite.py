import glob
import json


class BaseCheckSuite(object):
    """
    Base Check Suite class.
    """

    def __init__(self, _config):
        """
        :param _config: The configuration.
        """
        self.params = {}


def load_params(_dir):
    """
    Load parameter maps.

    :param _dir: The subdirectory of the parameter map.
    :return: A dictionary with the parameters.
    """
    params = {}
    for path in glob.glob(f'healthcheck/parameter_maps/{_dir}/*.json'):
        with open(path) as file:
            params[path] = json.loads(file.read())
    return params
