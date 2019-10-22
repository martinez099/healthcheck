import base64
import json
import math
import logging
import ssl

from urllib import request
from subprocess import Popen, PIPE

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

GB = pow(1024, 3)

black = lambda text: '\033[0;30m' + text + '\033[0m'
red = lambda text: '\033[0;31m' + text + '\033[0m'
green = lambda text: '\033[0;32m' + text + '\033[0m'
yellow = lambda text: '\033[0;33m' + text + '\033[0m'
blue = lambda text: '\033[0;34m' + text + '\033[0m'
magenta = lambda text: '\033[0;35m' + text + '\033[0m'
cyan = lambda text: '\033[0;36m' + text + '\033[0m'
white = lambda text: '\033[0;37m' + text + '\033[0m'


def to_kops(_value):
    """
    Concert a numeric value in readable ops/sec

    :param _value:
    :return:
    """
    return '{}K ops/sec'.format(math.ceil(_value / 1000))


def to_gb(_value):
    """
    Convert a numeric value from bytes to gigabytes.

    :param _value: A numeric value in bytes.
    :return: The numeric value in gigabytes.
    """
    return math.floor(_value / GB)


def exec_cmd(_args):
    """
    Execute a command in a subprocess.

    :param _args: The command string.
    :return: The response.
    :raise Exception: If an error occurred.
    """
    logging.debug('executing comand {}'.format(_args))
    proc = Popen(_args, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    proc.wait()
    if proc.returncode == 0:
        rsp = proc.stdout.read().decode('utf-8')
        return rsp.strip()
    else:
        rsp = proc.stderr.read().decode('utf-8')
        raise Exception(f'error during ssh remote execution (return code {proc.returncode}): {rsp.strip()}')


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
    if rsp.code == 200:
        return json.loads(rsp.read())
    else:
        raise Exception(f'error during http request (return code {rsp.code}): ' + rsp.read())


def get_parameter_map_name(_path):
    """
    Get the name of a parameter map out of its file path.

    :param _path: The file path of the parameter map.
    :return: The name of the parameter map.
    """
    fname = _path.split('/')[-1:]
    return fname[0].split('.')[0]
