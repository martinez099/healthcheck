import os

from utils import do_http


CLUSTER_FQDN = os.getenv('CLUSTER_FQDN')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

CACHE = {}


def fetch(_topic):
    if _topic in CACHE:
        return CACHE[_topic]
    else:
        rsp = do_http('https://{}:9443/v1/{}'.format(CLUSTER_FQDN, _topic), USERNAME, PASSWORD)
        CACHE[_topic] = rsp
        return rsp
