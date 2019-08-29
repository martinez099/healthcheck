import os
import pprint

from utils import urlopen, get_result


CLUSTER_FQDN = os.getenv('CLUSTER_FQDN')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')


rsp = urlopen('https://{}:9443/v1/cluster'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
pprint.pprint('Cluster {}'.format(CLUSTER_FQDN))
pprint.pprint(get_result(rsp))

rsp = urlopen('https://{}:9443/v1/nodes'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
pprint.pprint('Nodes:')
pprint.pprint(get_result(rsp))

rsp = urlopen('https://{}:9443/v1/bdbs'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
pprint.pprint('Databases:')
pprint.pprint(get_result(rsp))

rsp = urlopen('https://{}:9443/v1/shards'.format(CLUSTER_FQDN), USERNAME, PASSWORD)
pprint.pprint('Shards:')
pprint.pprint(get_result(rsp))
