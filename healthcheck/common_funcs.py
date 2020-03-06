import base64
import json
import logging
import socket
import ssl

from subprocess import run
from urllib import request

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

GB = pow(1024, 3)


def to_percent(_value):
    """
    Convert a numeric value to percentage.

    :param _value: The numeric value in decimal.
    :return:
    """
    return '{:.1f}'.format(_value * 100)


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
    completed_process = run(_args, shell=_shell, check=True, capture_output=True, encoding='utf-8')

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


def redis_ping(host, port, auth=None):
    """
    PING a Redis database.

    :param host: A Redis database host.
    :param port: A Redis database port.
    :param auth: An optional Redis database password.
    :return: True on success, False otherwise, error message on error.
    """
    conn = None
    try:
        conn = socket.create_connection((host, port))
        if auth:
            sent = conn.send(b'AUTH ' + auth.encode() + b'\r\n')
            if not sent:
                raise Exception('could not send AUTH to Redis server')

            recv = conn.recv(3)
            if not recv == b'+OK':
                raise Exception('invalid AUTH value sent to Redis server')

        sent = conn.send(b'PING\r\n')
        if not sent:
            raise Exception('could not send PING message to Redis server')

        return conn.recv(5) == b'+PONG'

    except Exception as e:
        return str(e)

    finally:
        if conn:
            conn.close()
