import weakref
import logging
import itertools
from common import *

class ETagEvent(object):
    Unset = 0
    Set   = 1
    Reset = 2

TagEventName = {
    ETagEvent.Unset: 'unset',
    ETagEvent.Set:   'set',
    ETagEvent.Reset: 'reset',
}

class ICallbackAcceptor(object):
    def AcceptCallback(self, reference, event):
        methName = "On" + event.title().replace("_", "")
        fn = getattr(self, methName, None)
        if callable(fn):
            fn(reference)
        else:
            logging.warning("can't invoke %s method for object %s", methName, self)


class CallbackHolder(Unpickable(callbacks=weakref.WeakKeyDictionary,
                                nonpersistent_callbacks=weakref.WeakKeyDictionary)):
    def AddCallbackListener(self, obj):
        if not isinstance(obj, ICallbackAcceptor):
            raise RuntimeError("callback %r\tcan't use object %r as acceptor" % (self, obj))
        self.callbacks[obj] = 1

    def AddNonpersistentCallbackListener(self, obj):
        if not isinstance(obj, ICallbackAcceptor):
            raise RuntimeError("callback %r\tcan't use object %r as acceptor" % (self, obj))
        self.nonpersistent_callbacks[obj] = 1

    def DropCallbackListener(self, obj):
        if obj in self.callbacks:
            del self.callbacks[obj]
        if obj in self.nonpersistent_callbacks:
            del self.nonpersistent_callbacks[obj]

    def FireEvent(self, event, reference=None):
        bad_listeners = set()
        for obj in itertools.chain(self.callbacks.keyrefs(), self.nonpersistent_callbacks.keyrefs()):
            if isinstance(obj(), ICallbackAcceptor):
                obj().AcceptCallback(reference or self, event)
            else:
                logging.warning("callback %r\tincorrect acceptor for %s found: %s", self, event, obj())
                bad_listeners.add(obj())
        for obj in bad_listeners:
            self.DropCallbackListener(obj)

    def GetListenersNumber(self):
        return len(self.callbacks)

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict['callbacks'] = dict(sdict['callbacks'].items())
        del sdict["nonpersistent_callbacks"]
        return sdict


class TagBase(CallbackHolder):
    def __init__(self, modify):
        CallbackHolder.__init__(self)
        self.done = False
        self._request_modify = modify

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)
        sdict.pop('_request_modify')
        return sdict

    def IsSet(self):
        return self.done

    #def __nonzero__(self):
        #return self.done

    def _Set(self):
        logging.debug("tag %s\tset", self.GetFullname())
        self.done = True
        self.FireEvent("done")

    def _Unset(self):
        logging.debug("tag %s\tunset", self.GetFullname())
        self.done = False
        self.FireEvent("undone")

    def _Reset(self, msg):
        logging.debug("tag %s\treset", self.GetFullname())
        self.done = False
        self.FireEvent("reset", (self, msg))

    def _ModifyLocalState(self, event, msg=None, version=None):
        if version is not None: # TODO Kosher
            self.version = version

        if event == ETagEvent.Set:
            self._Set()
        elif event == ETagEvent.Unset:
            self._Unset()
        elif event == ETagEvent.Reset:
            self._Reset(msg)

#
    def Set(self):
        self._request_modify(True, self, ETagEvent.Set)

    def Unset(self):
        self._request_modify(True, self, ETagEvent.Unset)

    def Reset(self, msg):
        self._request_modify(True, self, ETagEvent.Reset, msg)

    def Modify(self, event, msg=None):
        self._request_modify(True, self, event, msg)
#
# FIXIM Willn't use

    #def SetSafe(self):
        #self._request_modify(True, self, ETagEvent.Set)

    #def UnsetSafe(self):
        #self._request_modify(True, self, ETagEvent.Unset)

    #def ResetSafe(self, msg):
        #self._request_modify(True, self, ETagEvent.Reset, msg)

    #def ModifySafe(self, event, msg=None):
        #self._request_modify(True, self, event, msg)
#
    #def SetUnsafe(self):
        #return self._request_modify(False, self, ETagEvent.Set)

    #def UnsetUnsafe(self):
        #return self._request_modify(False, self, ETagEvent.Unset)

    #def ResetUnsafe(self, msg):
        #return self._request_modify(False, self, ETagEvent.Reset, msg)

    #def ModifyUnsafe(self, event, msg=None):
        #return self._request_modify(False, self, event, msg)
#

    def GetListenersIds(self):
        return [k.id for k in self.callbacks.iterkeys()]

    def IsRemote(self):
        return False

    def IsCloud(self):
        return False

    def CheckRemote(self):
        if not self.IsRemote():
            raise RuntimeError("Tag is not RemoteTag")
        return self


class LocalTag(TagBase):
    def __init__(self, name, modify):
        TagBase.__init__(self, modify)
        self.name = name

    def GetName(self):
        return self.name

    def GetFullname(self):
        return self.name


class RemoteTag(TagBase):
    def __init__(self, name, modify):
        TagBase.__init__(self, modify)
        self.remotehost, self.name = name.split(":")

    def Set(self):
        raise RuntimeError("Attempt to set RemoteTag %r", self)

    def Unset(self, message):
        raise RuntimeError("Attempt to unset RemoteTag %r", self)

    def Reset(self, message):
        raise RuntimeError("Attempt to reset RemoteTag %r", self)

    def GetRemoteHost(self):
        return self.remotehost

    def GetName(self):
        return self.name

    def GetFullname(self):
        return ':'.join((self.remotehost, self.name))

    def IsRemote(self):
        return True


class CloudTag(TagBase):
    def __init__(self, name, modify):
        TagBase.__init__(self, modify)
        self.name = name
        self.version = 0 # FIXME None

    # FIXME Changes will be applied serialized by tagname hash,
    # so no lock is need here

    def GetName(self):
        return self.name

    def GetFullname(self):
        return self.name

    def IsCloud(self):
        return True


"""Unpickler helper"""


def tagset(st=None):
    return set((v.name if isinstance(v, TagBase) else v) for v in st) if st else set()
