import subprocess
import re
import logging as system_logging
from collections import deque
import time

import rem.load_balancing as load_balancing

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

load_balancing.logging = logger


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


#class SkybitOps(object):
    #@classmethod
    #def _do_connect(cls, (host, port)):
        #logging.debug('before ProtobufConnection(%s, %s)' % (host, port))
        #ret = ProtobufConnection(host, port)
        #logging.debug('after ProtobufConnection(%s, %s)' % (host, port))
        #return ret

    #@classmethod
    #def _list_expr(cls, expr):
        #logging.debug('before list(%s)' % expr)
        #ret = [(host, port) for host, port, _ in sky_list_instances(expr)]
        #logging.debug('after list(%s)' % expr)
        #return ret

    #@classmethod
    #def local_and_remote(cls, local_expr, remote_expr):
        #def list_instances():
            #return (
                #cls._list_expr(local_expr),
                #cls._list_expr(remote_expr)
            #)

    ## XXX TODO XXX TODO XXX
        #def list_instances():
            #return (
                #[('sas1-6818.search.yandex.net', 8360)],
                #[('ws29-033.yandex.ru', 8600)],
            #)

        #instances = load_balancing.LocalAndRemoteInstances(list_instances)

        #return load_balancing.ConnectionFromInstances(instances, cls._do_connect)




import socket
class ProtobufConnection(object): # XXX FAKE
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sock = socket.create_connection((host, port))._sock


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
