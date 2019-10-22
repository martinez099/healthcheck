from healthcheck.common_funcs import http_get


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

    def get_with_value(self, _topic, _key, _value):
        """
        Get a topic with a given value.

        :param _topic: The topic, e.g. 'nodes'
        :param _key: The key, e.g. 'uid'
        :param _value: The value.
        :return: The result dictionary.
        """
        return filter(lambda x: x[_key] == _value, self._fetch(_topic))

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
            rsp = http_get('https://{}:9443/v1/{}'.format(self.fqdn, _topic), self.username, self.password)
            self.cache[_topic] = rsp
            return rsp
