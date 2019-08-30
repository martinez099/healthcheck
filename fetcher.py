import base64
import json
import logging
import os
import re
import ssl

from sshrex import exec_on_all_nodes, exec_on_node
from urllib import request

FQDN = os.getenv('CLUSTER_FQDN')
USERNAME = os.getenv('CLUSTER_USERNAME')
PASSWORD = os.getenv('CLUSTER_PASSWORD')

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE

CACHE = {}


def do_http(_url, _user, _pass, _data=None, _method='GET'):
    """
    Perfrom an HTTP request and get the JSON response.

    :param _url: The url of the request.
    :param _user: The username.
    :param _pass: The password.
    :param _data: The request body, defaults to None
    :param _method: The request method, defaults to GET.
    :raise Exception: In case of non-200 HTTP status code.
    :return: The response of the request.
    """
    req = request.Request(_url, method=_method)

    credentials = ('%s:%s' % (_user, _pass))
    encoded_credentials = base64.b64encode(credentials.encode('ascii'))
    req.add_header('Authorization', 'Basic %s' % encoded_credentials.decode("ascii"))

    if _data:
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        req.data(json.dumps(_data).encode('utf-8'))

    logging.info('calling urlopen to {} ...'.format(_url))
    rsp = request.urlopen(req, context=SSL_CONTEXT)
    if rsp.code == 200:
        return json.loads(rsp.read())
    else:
        raise Exception(str(rsp))


def fetch(_topic):
    if _topic in CACHE:
        return CACHE[_topic]
    else:
        rsp = do_http('https://{}:9443/v1/{}'.format(FQDN, _topic), USERNAME, PASSWORD)
        CACHE[_topic] = rsp
        return rsp


def fetch_shards_limit():
    rsp = fetch('license')
    match = re.search(r'Shards limit : (\d+)\n', rsp['license'], re.MULTILINE | re.DOTALL)
    return match.group(1)


def fetch_shards_count():
    rsp = fetch('shards')
    return len(rsp)


def fetch_number_of_nodes():
    rsp = fetch('nodes')
    return len(rsp)


def fetch_number_of_cores():
    rsp = fetch('nodes')
    return sum([node['cores'] for node in rsp])


def fetch_ram_size():
    rsp = fetch('nodes')
    return sum([node['total_memory'] for node in rsp])


def fetch_ephemeral_storage():
    rsp = fetch('nodes')
    return sum([node['ephemeral_storage_size'] for node in rsp])


def fetch_persistent_storage():
    rsp = fetch('nodes')
    return sum([node['persistent_storage_size'] for node in rsp])


def fetch_log_file_paths():
    return exec_on_all_nodes('df -h /var/opt/redislabs/log')


def fetch_tmp_file_path():
    return exec_on_all_nodes('df -h /tmp')


def fetch_software_versions():
    rsp = fetch('nodes')
    return [r['software_version'] for r in rsp]


def fetch_quorums():
    rsp = fetch('nodes')
    cmd = 'sudo /opt/redislabs/bin/rladmin info node {} | grep quorum'
    return [exec_on_node(cmd.format(i), 0) for i in range(1, len(rsp) + 1)]
