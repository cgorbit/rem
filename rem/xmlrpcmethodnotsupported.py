import xmlrpclib
import re

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

class ServerProxy(xmlrpclib.ServerProxy):
    def __getattr__(self, name):
        return Method(xmlrpclib.ServerProxy.__getattr__(self, name))

