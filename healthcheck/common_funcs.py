import base64
import functools
import json
import math
import logging
import re
import socket
import ssl

from subprocess import run, PIPE
from urllib import request

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

GB = pow(1024, 3)


def calc_usage(_values, _key):
    """
    Calculate minimum, average, maximum and standard deviation.

    :param _values: A list of value dicts.
    :param _key: The key of the value.
    :return: A tuple (minimum, average, maximum, standard deviation).
    """
    vals = list(filter(lambda x: x.get(_key), _values))
    min_ = min([i[_key] for i in vals])
    avg = sum(i[_key] for i in vals) / len(vals)
    max_ = max([i[_key] for i in filter(lambda i: i.get(_key), vals)])
    q_sum = functools.reduce(lambda x, y: x + pow(y[_key] - avg, 2), vals, 0)
    std_dev = math.sqrt(q_sum / len(vals))

    return min_, avg, max_, std_dev


def parse_semver(_version):
    """
    Parse a semantic version string, e.g. '5.6.0-20'.

    :param _version: The version string.
    :return: A tuple (major, minor, patch, build).
    """
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)-(\d+)$', _version)

    return tuple(map(int, match.groups()))


def to_percent(_value):
    """
    Convert a numeric value to percentage.

    :param _value: The numeric value in decimal.
    :return:
    """
    return '{:.1f}'.format(_value)


def to_kops(_value):
    """
    Convert a numeric value from ops to Kops

    :param _value: The numeric value in ops.
    :return: The rounded numeric value in Kops.
    """
    return '{:.1f}'.format(_value / 1000)


def to_gb(_value):
    """
    Convert a numeric value from bytes to gigabytes.

    :param _value: A numeric value in bytes.
    :return: The roundd numeric value in gigabytes.
    """
    return '{:.1f}'.format(_value / GB)


def to_ms(_value):
    """
    Just round for 3 digits.

    :param _value: A numeric value in milliseconds.
    :return: The numeric value in milliseconds.
    """
    return '{:.3f}'.format(_value)


def exec_cmd(_args, _shell=True):
    """
    Execute a command in a subprocess.

    :param _args: The command string.
    :param _shell: If a shell is used.
    :return: The response.
    :raise Exception: If an error occurred.
    """
    logging.debug('executing comand {}'.format(_args))
    completed_process = run(_args, shell=_shell, check=True, stdout=PIPE, stderr=PIPE, universal_newlines=True)

    return completed_process.stdout.strip()


def http_get(_url, _user, _pass):
    """
    Perfrom a HTTP GET request.

    :param _url: The url of the request.
    :param _user: The username.
    :param _pass: The password.
    :raise Exception: In case of non-200 HTTP status code.
    :return: The JSON response of the request.
    """
    req = request.Request(_url, method='GET')

    # set basic auth header
    credentials = ('%s:%s' % (_user, _pass))
    encoded_credentials = base64.b64encode(credentials.encode('ascii'))
    req.add_header('Authorization', 'Basic %s' % encoded_credentials.decode("ascii"))

    logging.debug('calling urlopen {} ...'.format(_url))
    rsp = request.urlopen(req, context=SSL_CONTEXT)
    if rsp.code != 200:
        raise Exception(f'error during http request (return code {rsp.code}): ' + rsp.read())

    return json.loads(rsp.read())


def get_parameter_map_name(_path):
    """
    Get the name of a parameter map out of its file path.

    :param _path: The file path of the parameter map.
    :return: The name of the parameter map.
    """
    return _path.split('/')[-1:][0].split('.')[0]


def redis_ping(_host, _port, auth=None):
    """
    PING a Redis database.

    :param _host: A Redis database host.
    :param _port: A Redis database port.
    :param auth: An optional Redis database password.
    :return: True on success, False otherwise, error message on error.
    """
    conn = None
    try:
        conn = socket.create_connection((_host, _port))
        if auth:
            sent = conn.send(b'AUTH ' + auth.encode() + b'\r\n')
            if not sent:
                raise Exception('could not send AUTH to Redis server')

            recv = conn.recv(5)
            if not recv == b'+OK\r\n':
                raise Exception('invalid AUTH value sent to Redis server')

        sent = conn.send(b'PING\r\n')
        if not sent:
            raise Exception('could not send PING message to Redis server')

        recv = conn.recv(7)

        # accept endpoint even if the provided password is invalid
        if recv == b'-NOAUTH':
            _ = conn.recv(2)
            return True

        return recv == b'+PONG\r\n' or recv

    except Exception as e:
        return str(e)

    finally:
        if conn:
            conn.close()


def is_api_configured(config):
    """
    Check if an [api] section was found in the configuration file.

    :param config:
    :return: Boolean
    """
    return 'api' in config


def is_rex_configured(config):
    """
    Check if a [ssh], [docker] or [k8s] section was found in the configuration file.

    :param config:
    :return: Boolean
    """
    return any(map(lambda x: x in config, ['ssh', 'docker', 'k8s']))
