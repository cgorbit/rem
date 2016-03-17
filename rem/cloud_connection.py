import re

import rem.nanny
import rem.load_balancing as load_balancing
from rem.cloud_client import ProtobufConnection
from rem.common import parse_network_address
from rem_logging import logger as logging


def _make_protobuf_connection(host, port):
    logging.debug('before ProtobufConnection(%s, %s)' % (host, port))
    ret = ProtobufConnection(host, port)
    logging.debug('after ProtobufConnection(%s, %s)' % (host, port))
    return ret


def fixed_plain_addr_list(addrs):
    list = lambda : addrs
    get = load_balancing.PlainInstancesList(list)

    return load_balancing.ConnectionFromInstances(
        get, lambda (host, port): _make_protobuf_connection(host, port)
    )


def nanny_service(nanny, service):
    list = lambda : nanny.list_instances(service)

    get = load_balancing.PlainInstancesList(list)
    #get = load_balancing.LocalAndRemoteInstances(lambda : (list(), []))

    return load_balancing.ConnectionFromInstances(
        get, lambda (host, port): _make_protobuf_connection(host, port)
    )


def single_host_port(host, port):
    return lambda : _make_protobuf_connection(host, port)


def single_addr(addr):
    host, port = parse_network_address(addr)
    return single_host_port(host, port)


def from_description(descr):
    # nanny://token@nanny.yandex-team.ru/prod_rem_cloud_tags_proxy
    if descr.startswith('nanny://'):
        m = re.match('^nanny://([a-z0-9]+)@([^/]+)/([\w-]+)$', descr)
        if not m:
            raise ValueError("Malformed nanny description '%s'" % descr)
        token, host, service = m.groups()
        nanny = rem.nanny.Nanny(host, token)
        print nanny.__dict__
        return nanny_service(nanny, service)

    # host:port
    # host0:port0,host0:port0
    else:
        addrs = [parse_network_address(addr) for addr in descr.split(',')]

        return single_host_port(*addrs[0]) if len(addrs) == 1 \
            else fixed_plain_addr_list(addrs)
