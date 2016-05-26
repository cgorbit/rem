import re
import socket
import xmlrpclib
import httplib
import struct
import threading
import select
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer as SimpleXMLRPCServerOrig
from SocketServer import ThreadingMixIn
import Queue as StdQueue
from rem_logging import logger as logging
from collections import deque
from profile import ProfiledThread

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


def _socket_set_linger(sock, l_onoff, l_linger):
    sock.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_LINGER,
        struct.pack('ii', l_onoff, l_linger)
    )

def _socket_send_reset(sock):
    _socket_set_linger(sock, 1, 0)


class AsyncXMLRPCServer2(SimpleXMLRPCServer):
    def __init__(self, pool_size, *args, **kws):
        SimpleXMLRPCServer.__init__(self, *args, **kws)
        listen_port = self.server_address[1]

        self._incoming = StdQueue.Queue(pool_size)
        self._main_thread = ProfiledThread(
            target=self.__serve_forever,
            name_prefix='RPCAcc-%d' % listen_port
        )
        self._workers_threads = [
            ProfiledThread(
                target=self.__worker,
                name_prefix='RPCWrk-%d' % listen_port
            )
                for _ in xrange(pool_size)
        ]
        self.__is_stopped = threading.Event()
        self.__should_stop = False

    def __worker(self):
        while True:
            task = self._incoming.get()
            if task is None:
                return

            self.__handle_request(*task)
            del task

    def __handle_request(self, sock, addr):
        try:
            self.finish_request(sock, addr)
            self.shutdown_request(sock)
        except:
            self.handle_error(sock, addr)
            self.shutdown_request(sock)

    def start(self):
        for t in self._workers_threads:
            t.start()
        self._main_thread.start()

    def serve_forever(self, poll_interval=0.5):
        raise NotImplementedError()

    def __serve_forever(self, poll_interval=0.5):
        while not self.__should_stop:
            r, w, e = select.select([self], [], [], poll_interval)
            if self in r:
                self.__put_request()

        self.server_close()

        self.__stop_workers()
        self.__reset_requests()

        self.__is_stopped.set()

    def __stop_workers(self):
        for _ in xrange(len(self._workers_threads)):
            self._incoming.put(None)

        for t in self._workers_threads:
            t.join()

    def __reset_requests(self):
        rest, self._incoming.queue = self._incoming.queue, deque()

        for sock, _ in rest:
            try:
                _socket_send_reset(sock)
            except Exception as e:
                logging.error("Failed to send RST to RPC client: %s" % e)

    def __put_request(self):
        try:
            request = self.get_request()
        except socket.error:
            logging.error("XMLRPCServer: socket error")
            return

    def shutdown(self):
        self.__should_stop = True
        self.__is_stopped.wait()


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


def is_xmlrpc_exception(exc, type):
    return isinstance(exc, xmlrpclib.Fault) and type.__name__ in exc.faultString


class RpcUserError(Exception):
    def __init__(self, exc):
        self.exc = exc

    def __repr__(self):
        return repr(self.exc)

    def __str__(self):
        return str(self.exc)


def as_rpc_user_error(from_rpc, exc):
    return RpcUserError(exc) if from_rpc else exc


def traced_rpc_method(level="debug"):
    log_method = getattr(logging, level)
    assert callable(log_method)

    def traced_rpc_method(func):
        def f(*args):
            try:
                return func(*args)
            except RpcUserError:
                _, e, tb = sys.exc_info()
                e = e.exc
                raise type(e), e, tb
            except:
                logging.exception("RPC method %s failed" % func.__name__)
                raise

        f.log_level = level
        f.__name__ = func.__name__
        f.__module__ = func.__module__

        return f

    return traced_rpc_method

