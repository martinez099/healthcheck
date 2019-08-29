import json
import base64
import ssl

from urllib import request


SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.CERT_NONE


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

    rsp = request.urlopen(req, context=SSL_CONTEXT)
    if rsp.code == 200:
        return json.loads(rsp.read())
    else:
        raise Exception(str(rsp))
