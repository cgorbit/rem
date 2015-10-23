import weakref
import logging
import itertools
from common import *

class TagEvent(object):
    Unset = 0
    Set   = 1
    Reset = 2

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


class Tag(CallbackHolder):
    def __init__(self, tagname):
        CallbackHolder.__init__(self)
        self.done = False
        self.name = tagname

    def IsSet(self):
        return self.done

    def _Set(self):
        logging.debug("tag %s\tset", self.name)
        self.done = True
        self.FireEvent("done")

    def _Unset(self):
        """unset function without event firing"""
        logging.debug("tag %s\tunset", self.name)
        self.done = False
        self.FireEvent("undone")

    def _Reset(self, message):
        logging.debug("tag %s\treset", self.name)
        self.done = False
        self.FireEvent("reset", (self, message))

    def Set(self):
        self._Set()

    def Unset(self):
        self._Unset()

    def Reset(self, message):
        self._Reset(message)

    def _Modify(self, event, message=None):
        if event == TagEvent.Set:
            self._Set()
        elif event == TagEvent.Unset:
            self._Unset()
        elif event == TagEvent.Reset:
            self._Reset(message)

    def GetName(self):
        return self.name

    def GetFullname(self):
        return self.name

    def IsRemote(self):
        return False

    def CheckRemote(self):
        if not self.IsRemote():
            raise RuntimeError("Tag is not RemoteTag")
        return self

    def GetListenersIds(self):
        return [k.id for k in self.callbacks.iterkeys()]


class RemoteTag(Tag):
    def __init__(self, tagname):
        Tag.__init__(self, tagname)
        self.remotehost, self.name = tagname.split(":")

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


"""Unpickler helper"""


def tagset(st=None):
    return set((v.name if isinstance(v, Tag) else v) for v in st) if st else set()
