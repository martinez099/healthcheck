import base64
import json
import logging
import ssl

from urllib import request

SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE


class ApiFetcher(object):
    """
    API-Fetcher class.
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

    def get(self, _topic):
        """
        Get a topic.

        :param _topic: The topic, e.g. 'nodes'
        :return: The result dictionary.
        """
        return self._fetch(_topic)

    def get_value(self, _topic, _key):
        """
        Get a value from a topic.

        :param _topic: The topic, e.g. 'nodes'
        :param _key: The key of the value.
        :return: The value.
        """
        return self._fetch(_topic)[_key]

    def get_values(self, _topic, _key):
        """
        Get values from a topic.

        :param _topic: The topic, e.g. 'nodes'
        :param _key: The key of the values.
        :return: A list with values.
        """
        return [node[_key] for node in self._fetch(_topic)]

    def get_number_of_values(self, _topic):
        """
        Get the amount of values from a topic.

        :param _topic: The topic, e.g. 'nodes'
        :return: The amount of values.
        """
        return len(self._fetch(_topic))

    def get_sum_of_values(self, _topic, _key):
        """
        Get the sum of values from a topic.

        :param _topic: The topic, e.g. 'nodes'
        :param _key: The key of the values.
        :return: The sum of the values.
        """
        return sum([node[_key] for node in self._fetch(_topic)])

    def _fetch(self, _topic):
        """
        Fetch a topic.

        :param _topic: The topic, e.g. 'nodes'
        :return: The result dictionary.
        """
        if _topic in self.cache:
            return self.cache[_topic]
        else:
            rsp = self._do_http('https://{}:9443/v1/{}'.format(self.fqdn, _topic), self.username, self.password)
            self.cache[_topic] = rsp
            return rsp

    @staticmethod
    def _do_http(_url, _user, _pass):
        """
        Perfrom an HTTP GET request.

        :param _url: The url of the request.
        :param _user: The username.
        :param _pass: The password.
        :raise Exception: In case of non-200 HTTP status code.
        :return: The JSON response of the request.
        """
        req = request.Request(_url, method='GET')

        # basic auth header
        credentials = ('%s:%s' % (_user, _pass))
        encoded_credentials = base64.b64encode(credentials.encode('ascii'))
        req.add_header('Authorization', 'Basic %s' % encoded_credentials.decode("ascii"))

        logging.debug('calling urlopen to {} ...'.format(_url))
        rsp = request.urlopen(req, context=SSL_CONTEXT)
        if rsp.code == 200:
            return json.loads(rsp.read())
        else:
            raise Exception(f'error during http request (return code {rsp.code}): ' + rsp.read())
