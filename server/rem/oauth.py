import requests
import urllib2
import requests.exceptions as exceptions
from rem.rem_logging import logger as logging
import time


class OAuthInfo(object):
    def __init__(self):
        self.token = None
        self.client_id = None
        self.login = None
        self.token_status = None
        self.error = None


class TokenStatus(object):
    VALID = 'VALID'


def parse_oauth_header(header):
    type, rest = header.split(' ', 1)

    if type != 'OAuth':
        raise ValueError("Authorization is not of OAuth type")

    return urllib2.parse_keqv_list(urllib2.parse_http_list(rest))


class TemporaryError(RuntimeError):
    pass


class BlackboxServerError(TemporaryError):
    pass


class NetworkError(TemporaryError):
    pass


MIN_FETCH_INTERVAL   =  1.0
MAX_FETCH_INTERVAL   = 16.0
MAX_TOTAL_FETCH_TIME = 60.0
FETCH_TIMEOUT = 2.0


def get_token_info(token):
    # FIXME use https (it's slower: 20ms vs 5ms)
    url = 'http://blackbox.yandex-team.ru/blackbox?method=oauth' \
          '&oauth_token={token}&userip={addr}&format=json'

    url = url.format(token=token, addr='127.0.0.1')

    last_error = None

    delay = MIN_FETCH_INTERVAL
    deadline = time.time() + MAX_TOTAL_FETCH_TIME

    while True:
        ok = False

        now = time.time()
        if now > deadline:
            if last_error:
                raise last_error
            else:
                fetch_timeout = 0.0
        else:
            fetch_timeout = min(FETCH_TIMEOUT, deadline - now)

        try:
            resp = requests.get(url, timeout=fetch_timeout)
            ok = True
        except (exceptions.Timeout, exceptions.SSLError, exceptions.ConnectionError) as e:
            logging.warning('Failed to get answer from blackbox: %s' % e)
            last_error = NetworkError(str(e))
        else:
            if resp.status_code / 100 == 5: # doc says that blackbox always returns 200...
                ok = False
                msg = 'Got %d http status from blackbox' % resp.status_code
                last_error = BlackboxServerError(msg)
                logging.warning(msg)

        if ok:
            break

        now = time.time()
        if now > deadline:
            raise last_error

        #logging.debug('RETRY: %s' % last_error)
        delay = min(delay * 2, MAX_FETCH_INTERVAL, deadline - now)
        time.sleep(delay)

    if resp.status_code != 200:
        raise RuntimeError("blackbox answered with %s status code" % resp.status_code)

    resp = resp.json()

    ret = OAuthInfo()
    ret.token = token
    ret.error = resp.get('error')

    if 'status' in resp:
        ret.token_status = resp['status']['value']

        if ret.token_status == TokenStatus.VALID:
            ret.login = resp['login']
            oauth = resp['oauth']
            ret.client_id = oauth['client_id']
    else:
        raise RuntimeError(ret.error)

    return ret


def get_oauth_from_header(authorization):
    oauth_opts = parse_oauth_header(authorization)
    oauth_token = oauth_opts.get('oauth_token')
    if oauth_token is None:
        raise ValueError("No oauth_token in OAuth authorization header")

    return get_token_info(oauth_token)
