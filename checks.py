import os
import pprint
import re

from utils import do_http


CLUSTER_FQDN = os.getenv('CLUSTER_FQDN')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')


def get_cluster():
    rsp = do_http('https://{}:9443/v1/cluster'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    pprint.pprint('Cluster {}'.format(CLUSTER_FQDN))
    pprint.pprint(rsp)


def get_nodes():
    rsp = do_http('https://{}:9443/v1/nodes'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    pprint.pprint('Nodes:')
    pprint.pprint(rsp)


def get_bdbs():
    rsp = do_http('https://{}:9443/v1/bdbs'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    pprint.pprint('Databases:')
    pprint.pprint(rsp)


def get_shards():
    rsp = do_http('https://{}:9443/v1/shards'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    pprint.pprint('Shards:')
    pprint.pprint(rsp)


def check_license():
    rsp = do_http('https://{}:9443/v1/license'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    match = re.search(r'Shards limit : (\d+)\n', rsp['license'], re.MULTILINE | re.DOTALL)
    shards_limit = match.group(1)
    pprint.pprint('shards_limit: {}'.format(shards_limit))


def check_shards_count(bdb_id):
    rsp = do_http('https://{}:9443/v1/bdbs/{}'.format(CLUSTER_FQDN, bdb_id), USERNAME, PASSWORD)
    shards_count = rsp['shards_count']
    pprint.pprint('shards_count: {}'.format(shards_count))


def check_number_of_nodes():
    rsp = do_http('https://{}:9443/v1/nodes'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    number_of_nodes = len(rsp)
    pprint.pprint('number_of_nodes: {}'.format(number_of_nodes))


def check_number_of_cores():
    rsp = do_http('https://{}:9443/v1/nodes'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
    number_of_cores = sum([node['cores'] for node in rsp])
    pprint.pprint('number_of_cores: {}'.format(number_of_cores))
