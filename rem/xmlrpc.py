import logging
import re
import socket
import xmlrpclib
import httplib
from SimpleXMLRPCServer import SimpleXMLRPCServer as SimpleXMLRPCServerOrig
from SocketServer import ThreadingMixIn
import Queue as StdQueue

class SimpleXMLRPCServer(SimpleXMLRPCServerOrig):
    def __init__(self, *args, **kws):
        if socket.has_ipv6:
            self.address_family = socket.AF_INET6
        SimpleXMLRPCServerOrig.__init__(self, *args, **kws)

class AsyncXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    def __init__(self, poolsize, *args, **kws):
        SimpleXMLRPCServer.__init__(self, *args, **kws)
        self.poolsize = poolsize
        self.requests = StdQueue.Queue(poolsize)

    def handle_request(self):
        try:
            request = self.get_request()
        except socket.error:
            logging.error("XMLRPCServer: socket error")
            return
        if self.verify_request(*request):
            self.requests.put(request)

class XMLRPCMethodNotSupported(xmlrpclib.Fault):
    pass

class Method:
    def __init__(self, method):
        self.method = method

    def __getattr__(self, name):
        return Method(getattr(self.method, name))

    def __call__(self, *args, **kws):
        try:
            return self.method(*args, **kws)
        except xmlrpclib.Fault as e:
            if re.search('method "[^"]+" is not supported$', e.faultString):
                raise XMLRPCMethodNotSupported(e.faultCode, e.faultString)
            else:
                raise

class Transport(xmlrpclib.Transport):
    def __init__(self, use_datetime=0, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        xmlrpclib.Transport.__init__(self, use_datetime)
        self.__socket_timeout = timeout

    def make_connection(self, host):
        if self._connection and host == self._connection[0]:
            return self._connection[1]

        chost, self._extra_headers, x509 = self.get_host_info(host)

        self._connection = host, httplib.HTTPConnection(chost, timeout=self.__socket_timeout)
        return self._connection[1]

class ServerProxy(xmlrpclib.ServerProxy):
    def __init__(self, uri, transport=None, encoding=None, verbose=0,
                 allow_none=0, use_datetime=0,
                 timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                 **kws):
        if transport is None:
            transport = Transport(timeout=timeout)

        xmlrpclib.ServerProxy.__init__(self, uri, transport, encoding, verbose,
                              allow_none, use_datetime, **kws)

    def __getattr__(self, name):
        return Method(xmlrpclib.ServerProxy.__getattr__(self, name))
