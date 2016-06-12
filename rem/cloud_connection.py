import re

import rem.nanny
import rem.load_balancing as load_balancing
from rem.common import parse_network_address
from rem_logging import logger as logging

def _connection_from_instances(get_instance):
    return lambda connect: load_balancing.ConnectionFromInstances(get_instance, connect)


def fixed_plain_addr_list(addrs):
    list = lambda : addrs
    get = load_balancing.PlainInstancesList(list)
    return _connection_from_instances(get)


def nanny_service(nanny, service):
    list = lambda : nanny.list_instances(service)
    get = load_balancing.PlainInstancesList(list)
    return _connection_from_instances(get)


def single_host_port(host, port):
    return fixed_plain_addr_list([(host, port)]) # for timeouts in _Instance


def single_addr(addr):
    host, port = parse_network_address(addr)
    return single_host_port(host, port)


def from_description(descr):
    # nanny://token@nanny.yandex-team.ru/prod_rem_cloud_tags_proxy
    if descr.startswith('nanny://'):
        m = re.match('^nanny://([a-zA-Z0-9-]+)@([^/]+)/([\w-]+)$', descr)
        if not m:
            raise ValueError("Malformed nanny description '%s'" % descr)
        token, host, service = m.groups()
        nanny = rem.nanny.Nanny(host, token)
        return nanny_service(nanny, service)

    # host0:port0,host1:port1
    else:
        addrs = [parse_network_address(addr) for addr in descr.split(',')]
        return fixed_plain_addr_list(addrs)

        #return single_host_port(*addrs[0]) if len(addrs) == 1 \
            #else fixed_plain_addr_list(addrs)
