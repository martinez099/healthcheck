import pprint
import re

from fetcher import fetch


def check_license():
    rsp = fetch('license')
    match = re.search(r'Shards limit : (\d+)\n', rsp['license'], re.MULTILINE | re.DOTALL)
    shards_limit = match.group(1)
    pprint.pprint('shards_limit: {}'.format(shards_limit))


def check_shards_count(bdb_id):
    rsp = fetch('bdbs/{}'.format(bdb_id))
    shards_count = rsp['shards_count']
    pprint.pprint('shards_count: {}'.format(shards_count))


def check_number_of_nodes():
    rsp = fetch('nodes')
    number_of_nodes = len(rsp)
    pprint.pprint('number_of_nodes: {}'.format(number_of_nodes))


def check_number_of_cores():
    rsp = fetch('nodes')
    number_of_cores = sum([node['cores'] for node in rsp])
    pprint.pprint('number_of_cores: {}'.format(number_of_cores))


def check_ram_size():
    rsp = fetch('nodes')
    number_of_cores = sum([node['total_memory'] for node in rsp])
    pprint.pprint('total_memory: {}'.format(number_of_cores))


def check_ephemeral_storage():
    rsp = fetch('nodes')
    number_of_cores = sum([node['ephemeral_storage_size'] for node in rsp])
    pprint.pprint('ephemeral_storage_size: {}'.format(number_of_cores))


def check_persistent_storage():
    rsp = fetch('nodes')
    number_of_cores = sum([node['persistent_storage_size'] for node in rsp])
    pprint.pprint('persistent_storage_size: {}'.format(number_of_cores))
