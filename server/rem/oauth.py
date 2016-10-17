import requests
import urllib2


class OAuthInfo(object):
    def __init__(self):
        self.token = None
        self.client_id = None
        self.login = None
        self.token_status = None


class TokenStatus(object):
    VALID = 'VALID'


def parse_oauth_header(header):
    type, rest = header.split(' ', 1)

    if type != 'OAuth':
        raise ValueError("Authorization is not of OAuth type")

    return urllib2.parse_keqv_list(urllib2.parse_http_list(rest))


def get_token_info(token):
    ip_addr = "127.0.0.1" # FIXME

    # FIXME use https?
    url = 'http://blackbox.yandex-team.ru/blackbox?method=oauth' \
          '&oauth_token={token}&userip={addr}&format=json'

# TODO Retries
    resp = requests.get(url.format(token=token, addr=ip_addr))

    if resp.status_code != 200:
        raise RuntimeError("blackbox answered with %s status code" % resp.status_code)

    resp = resp.json()

    ret = OAuthInfo()
    ret.token = token

    error = resp['error']
    if error != 'OK':
        raise RuntimeError("Bad token: %s" % error)

    ret.login = resp['login']

    oauth = resp['oauth']

    ret.token_status = resp['status']['value']

    ret.client_id = oauth['client_id']

    return ret


def get_oauth_from_header(authorization):
    oauth_opts = parse_oauth_header(authorization)
    oauth_token = oauth_opts.get('oauth_token')
    if oauth_token is None:
        raise ValueError("No oauth_token in OAuth authorization header")

    return get_token_info(oauth_token)
