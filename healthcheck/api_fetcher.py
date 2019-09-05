import base64
import datetime
import json
import logging
import re
import ssl

from urllib import request

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE


class ApiFetcher(object):
    """
    API Fetcher class.
    """

    def __init__(self, _fqdn, _username, _password):
        """
        :param _fqdn: The FQDN of the cluster.
        :param _username: The username of the cluster.
        :param _password: The password of the cluster.
        """
        self.fqdn = _fqdn
        self.username = _username
        self.password = _password
        self.cache = {}

    def get_cluster_value(self, _key):
        return self._fetch('cluster')[_key]

    def get_license_shards_limit(self):
        rsp = self._fetch('license')
        match = re.search(r'Shards limit : (\d+)\n', rsp['license'], re.MULTILINE | re.DOTALL)
        return int(match.group(1))

    def get_license_expire_date(self):
        rsp = self._fetch('license')
        return datetime.datetime.strptime(rsp['expiration_date'], '%Y-%m-%dT%H:%M:%SZ')

    def get_license_expired(self):
        rsp = self._fetch('license')
        return rsp['expired']

    def get_number_of_shards(self):
        rsp = self._fetch('shards')
        return len(rsp)

    def get_number_of_nodes(self):
        rsp = self._fetch('nodes')
        return len(rsp)

    def get_node_values(self, _key):
        return [node[_key] for node in self._fetch('nodes')]

    def get_sum_of_node_values(self, _key):
        return sum([node[_key] for node in self._fetch('nodes')])

    def get_bdb_value(self, _bdb_id, _key):
        return self._fetch(f'bdbs/{_bdb_id}')[_key]

    def get_bdb_alerts(self, _bdb_id=None):
        return self._fetch(f'bdbs/alerts/{_bdb_id}') if _bdb_id else self._fetch('bdbs/alerts')

    def _fetch(self, _topic):
        if _topic in self.cache:
            return self.cache[_topic]
        else:
            rsp = self._do_http('https://{}:9443/v1/{}'.format(self.fqdn, _topic), self.username, self.password)
            self.cache[_topic] = rsp
            return rsp

    @staticmethod
    def _do_http(_url, _user, _pass, _data=None, _method='GET'):
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

        logging.debug('calling urlopen to {} ...'.format(_url))
        rsp = request.urlopen(req, context=SSL_CONTEXT)
        if rsp.code == 200:
            return json.loads(rsp.read())
        else:
            raise Exception(f'error during http request (return code {rsp.code}): ' + rsp.read())
