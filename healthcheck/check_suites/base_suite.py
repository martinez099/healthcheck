import math
import inspect

from healthcheck.api_fetcher import ApiFetcher
from healthcheck.ssh_rex import SshRemoteExecutor

GB = pow(1024, 3)


def to_gb(_value):
    """
    Convert a numeric value from bytes to gigabytes.

    :param _value: A numeric value in bytes.
    :return: The numeric value in gigabytes.
    """
    return math.floor(_value / GB)


def format_result(_result, **_kwargs):
    """
    Format a check result.

    :param _result: The result of the check.
    :param _kwargs: A dict with argument name->value pairs to render.
    :return: A string with the rendered result.
    """
    check_name = inspect.stack()[1][3]
    if _result:
        result = '[+] '
    elif _result is False:
        result = '[-] '
    else:
        result = '[~] '
    result += f'[{check_name}] '
    return result + f', '.join([k + ': ' + str(v) for k, v in _kwargs.items()])


class CheckSuite(object):
    """
    Check Suite class.
    """

    def __init__(self, _args):
        """
        :param _args: The parsed command line arguments.
        """
        self.api = ApiFetcher(_args.cluster_fqdn, _args.cluster_username, _args.cluster_password)
        self.ssh = SshRemoteExecutor(_args.ssh_username, _args.ssh_hostnames.split(','), _args.ssh_keyfile)
