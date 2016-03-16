import subprocess
import re
import logging as system_logging
from collections import deque
import time

_instance_re = re.compile('^([^:]+):(\d+)@(.+)$')

logger = system_logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(system_logging.StreamHandler())
logger.setLevel(system_logging.DEBUG)
logger.error('__init__')
logger.handlers[0].setFormatter(
    system_logging.Formatter("%(asctime)s %(levelname)-8s\t%(message)s")
)

logging = logger


def sky_list_instances(expr):
    proc = subprocess.Popen(
        'sky listinstances ' + expr,
        shell=True,
        #['sky', 'listinstances'] + \
            #['G@' + group for group in groups],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    out, err = proc.communicate()

    if proc.returncode:
        raise RuntimeError("sky listinstances exited with %s:\n%s" % (proc.returncode, err))

    ret = []

    for record in out.rstrip('\n').split('\n'):
        m = _instance_re.match(record)
        if not m:
            raise RuntimeError("Malformed record %r in listinstances" % record)

        host, port, branch = m.groups()

        ret.append((host, int(port), branch))

    return ret


class _InstanceGroup(object):
    class _Instance(object):
        FAILS_LEN = 3

        def __init__(self, addr):
            self.addr = addr
            self._fails = deque()
            self._last_cant_connect = 0

        def register_disconnect(self):
            logging.debug("++ register_disconnect(%s)" % (self.addr,))

            self._last_cant_connect = 0 # FIXME More kosher

            self._fails.append(time.time())

            if len(self._fails) > self.FAILS_LEN:
                self._fails.popleft()

        def register_cant_connect(self, error):
            logging.debug("++ register_cant_connect(%s, %s)" % (self.addr, error))
            #(void)error
            self._last_cant_connect = time.time()

    # XXX TODO XXX TODO XXX
        def is_good(self):
            if time.time() - self._last_cant_connect < 1:
                return False

            #FAILS_LEN = 3
            #fails = deque(iterable)

            fails = self._fails
            need_fails_len = self.FAILS_LEN

            #time.time = lambda : 100

            def pairs():
                cont = fails
                return [(cont[idx - 1], cont[idx]) for idx, _ in enumerate(cont) if idx]

            if len(fails) == need_fails_len:
                always_fail = all(v1 - v0 < 10 for v0, v1 in pairs())
                #print list(pairs())
                #print [v1 - v0 for v0, v1 in pairs()]
                #print [v1 - v0 < 10 for v0, v1 in pairs()]
                #print always_fail

                if always_fail and time.time() - fails[-1] < 30: # TODO
                    return False

            return True

    def __init__(self, addrs):
        self._instances = deque(self._Instance(addr) for addr in addrs)

    def update_instances(self, new_addrs):
        new_addrs = set(new_addrs)

        old_addrs = set(i.addr for i in self._instances)

        if old_addrs == new_addrs:
            return

        to_add = new_addrs - old_addrs

        self._instances = deque(
            [self._Instance(addr) for addr in to_add] + \
            [i for i in self._instances if i.addr in new_addrs]
        )

    def get(self):
        for idx, instance in enumerate(self._instances):
            if instance.is_good():
                if len(self._instances) > 1:
                    self._instances.rotate(-(idx + 1))
                return instance


class _PlainInstancesList(object):
    def __init__(self, list_instances, list_update_interval=600):
        self._list_instances = list_instances
        self._last_list_update_time = 0
        self._list_update_interval = list_update_interval
        self._list_update_interval = 10 # TODO REMOVE
        self._instances_group = None

    def _try_update_addrs(self):
        try:
            new_instances = self._list_instances()
        except Exception as e:
            logging.exception("Failed to get addrs")
            return

        logging.debug('_list_instances => %s;' % new_instances)

        if not new_instances:
            logging.error("Empty addrs list")
            return

        if not self._instances_group:
            self._instances_group = _InstanceGroup(new_instances)
        else:
            self._instances_group.update_instances(new_instances)

        self._last_list_update_time = time.time() # TODO

        return True

    def __call__(self):
        if not self._instances_group:
            logging.debug("++ if not self._instances_group")
            if not self._try_update_addrs():
                return

        if time.time() - self._last_list_update_time > self._list_update_interval:
            self._try_update_addrs()

        instance = self._instances_group.get()
        #logging.debug('instance_group give no instance')
        return instance


class _LocalAndRemoteInstances(object):
    _LOCAL_GROUP_MAX_WAIT_TIME = 60.0

    def __init__(self, list_addrs, list_update_interval=600):
        self._list_addrs = list_addrs
        self._last_list_update_time = 0
        self._list_update_interval = list_update_interval
        self._list_update_interval = 10 # TODO REMOVE
        self._local_group = None
        self._local_group_first_fail_time = None
        self._remote_group = None

    def _try_update_addrs(self):
        try:
            local, remote = self._list_addrs()
        except Exception as e:
            logging.exception("Failed to get addrs")
            return

        logging.debug('_list_addrs => local = %s; remote = %s' % (local, remote))

        if not local and not remote:
            logging.error("Empty addrs list")
            return

        if not local:
            logging.warning("No local addrs: %s" % ((local, remote),))
            return

        if not self._local_group:
            self._local_group = _InstanceGroup(local)
            self._remote_group = _InstanceGroup(remote)
        else:
            self._local_group.update_instances(local)
            self._remote_group.update_instances(remote)

        self._last_list_update_time = time.time() # TODO

        return True

    def __call__(self):
        if not self._local_group:
            logging.debug("++ if not self._local_group")
            if not self._try_update_addrs():
                return

        if time.time() - self._last_list_update_time > self._list_update_interval:
            self._try_update_addrs()

        instance = self._local_group.get()
        if instance:
            return instance

        logging.debug('local_group give no instance')

        if not self._local_group_first_fail_time:
            self._local_group_first_fail_time = time.time()

        if time.time() - self._local_group_first_fail_time < self._LOCAL_GROUP_MAX_WAIT_TIME:
            return

        instance = self._remote_group.get()
        if not instance:
            logging.debug('remote_group give no instance')

        return instance


class _ConnectionFromInstances(object):
    def __init__(self, get_instance, create_connection):
        self._get_instance = get_instance
        self._create_connection = create_connection
        self._current_instance = None

    def __call__(self):
        if self._current_instance:
            self._current_instance.register_disconnect()
            self._current_instance = None

        while True:
            logging.debug('_ConnectionFromInstances.while.True')

            instance = self._get_instance()
            if not instance:
                return

            try:
                connection = self._create_connection(instance.addr)
            except Exception as e:
                instance.register_cant_connect(e)
            else:
                break

        self._current_instance = instance
        return connection


import socket
class ProtobufConnection(object): # XXX FAKE
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sock = socket.create_connection((host, port))._sock


class SkybitOps(object):
    @classmethod
    def _do_connect(cls, (host, port)):
        logging.debug('before ProtobufConnection(%s, %s)' % (host, port))
        ret = ProtobufConnection(host, port)
        logging.debug('after ProtobufConnection(%s, %s)' % (host, port))
        return ret

    @classmethod
    def _list_expr(cls, expr):
        logging.debug('before list(%s)' % expr)
        ret = [(host, port) for host, port, _ in sky_list_instances(expr)]
        logging.debug('after list(%s)' % expr)
        return ret

    @classmethod
    def local_and_remote(cls, local_expr, remote_expr):
        def list_instances():
            return (
                cls._list_expr(local_expr),
                cls._list_expr(remote_expr)
            )

    # XXX TODO XXX TODO XXX
        def list_instances():
            return (
                [('sas1-6818.search.yandex.net', 8360)],
                [('ws29-033.yandex.ru', 8600)],
            )

        instances = _LocalAndRemoteInstances(list_instances)

        return _ConnectionFromInstances(instances, cls._do_connect)


class NannyOps(object):
    @classmethod
    def _do_connect(cls, (host, port)):
        logging.debug('before ProtobufConnection(%s, %s)' % (host, port))
        ret = ProtobufConnection(host, port)
        logging.debug('after ProtobufConnection(%s, %s)' % (host, port))
        return ret

    #@classmethod
    #def local_and_remote(cls, host):
        #def list_instances():
            #return (
                #cls._list_expr(local_expr),
                #cls._list_expr(remote_expr)
            #)

        #def list_instances():
            #return (
                #[('sas1-6818.search.yandex.net', 8360)],
                #[('ws29-033.yandex.ru', 8600)],
            #)

        #instances = _LocalAndRemoteInstances(list_instances)

        #return _ConnectionFromInstances(instances, cls._do_connect)

    @classmethod
    def _make_instances_lister(nanny, service):
        return lambda : nanny.list_instances(service)

    @classmethod
    def plain(cls, nanny, service):
        list = cls._make_instances_lister(nanny)
        #get = _PlainInstancesList(list,
        get = _LocalAndRemoteInstances(lambda : (list(), []))
        return _ConnectionFromInstances(get, cls._do_connect)


import requests
class Nanny(object):
    def __init__(self, host, token):
        self._host = host
        self._token = token

    def list_instances(self, srv):
        return [
            (instance['hostname'].encode('ascii'), int(instance['port']))
                for instance in self._request('%s/current_state/instances/' % srv)
        ]

    def _request(self, path):
        headers = {
            'Authorization': 'OAuth ' + self._token,
            'Content-Type': 'application/json',
        }

        r = requests.request(
            'GET',
            'http://' + self._host + '/v2/services/' + path,
            #data=json.dumps(data),
            headers=headers,
            verify=False
        )

        return r.json()['result']

def make_nanny_instances_lister(host, token, service):
    n = Nanny(host, token)
    def list():
        return n.list_instances(service)
    return list

if __name__ == '__main__':
    import sys

    list = make_nanny_instances_lister(
        host = 'nanny.yandex-team.ru',
        token = '47821a5802574cdf9e23db6d15ec9b0d',
        service = 'prod_rem_cloud_tags_proxy',
    )

    print list()
    print
    print list()
    print
    print list()

    sys.exit(0)

    # local = host1:port1, host2:port2; remote = sky://G@MAN_TEST_REM_CLOUD_TAGS_PROXY
    # sky://G@MAN_TEST_REM_CLOUD_TAGS_PROXY G@SAS_TEST_REM_CLOUD_TAGS_PROXY
    # host1:port2, host2:port2

    #print sky_list_instances('G@MAN_TEST_REM_CLOUD_TAGS_PROXY G@SAS_TEST_REM_CLOUD_TAGS_PROXY G@MSK_FOL_TEST_REM_CLOUD_TAGS_PROXY')

    #print sky_list_instances([
        #'MAN_TEST_REM_CLOUD_TAGS_PROXY',
        #'SAS_TEST_REM_CLOUD_TAGS_PROXY',
        #'MSK_FOL_TEST_REM_CLOUD_TAGS_PROXY',
    #])

    #ctor = SkybitOps.local_and_remote(
        #'G@SAS_TEST_REM_CLOUD_TAGS_PROXY',
        #'G@MSK_FOL_TEST_REM_CLOUD_TAGS_PROXY'
    #)

    ctor = SkybitOps.local_and_remote(
        'G@SAS_TEST_REM_CLOUD_TAGS_PROXY',
        'G@MSK_FOL_TEST_REM_CLOUD_TAGS_PROXY')

    c = ctor()

    from pprint import pprint

    print
    pprint(c.__dict__ if c is not None else 'None')
    print
