#coding: utf-8
import select
import socket
import bsddb3
from ConfigParser import ConfigParser
import cPickle
from collections import deque

from xmlrpc import ServerProxy as XMLRPCServerProxy, XMLRPCMethodNotSupported, SimpleXMLRPCServer
from common import *
from callbacks import TagBase, ICallbackAcceptor, ETagEvent, TagEventName
from rem.profile import ProfiledThread
from rem_logging import logger as logging
import rem.runproc as runproc

PROTOCOL_VERSION = 2

def _get_tags_to_set(events):
    return list(set(ev[0] for ev in events if ev[1] == ETagEvent.Set))

class Deque(object):
    # Thread-affinity: just as needed for ClientInfo

    def __init__(self, rhs=None):
        self._impl = rhs._impl if rhs else deque()

    def pop(self, count=1):
        for _ in xrange(count):
            self._impl.popleft()

    def push(self, el):
        self._impl.append(el)

    def as_list(self):
        return list(self._impl)

    def __getslice__(self, start, end):
        ret = list()
        for idx in xrange(start, min(end, len(self._impl))):
            ret.append(self._impl[idx])
        return ret

    def __repr__(self):
        return repr(self._impl)

    def __len__(self):
        return len(self._impl)

    def __nonzero__(self):
        return bool(self._impl)

class ClientInfo(Unpickable(events=Deque,
                            name=str,
                            subscriptions=set,
                            errorsCnt=(int, 0),
                            version=(int, PROTOCOL_VERSION),
                            send_events_lock=PickableLock,
                            active=(bool, True))):
    MAX_TAGS_BULK = 100
    PENALTY_FACTOR = 6
    SOCKET_TIMEOUT = 120.0

    def __init__(self, *args, **kws):
        getattr(super(ClientInfo, self), "__init__")(*args, **kws)
        self.update(*args, **kws)
        self.lastError = None

    def Connect(self):
        self.connection = XMLRPCServerProxy(self.systemUrl, allow_none=True,
                                            timeout=self.SOCKET_TIMEOUT)

    def RegisterTagEvent(self, tag, event, message=None):
        self.events.push((tag, event, message))

    def Subscribe(self, tagname):
        self.subscriptions.add(tagname)

    def GetEventsAsTagsToSet(self):
        return _get_tags_to_set(self.events.as_list())

    def GetEventsAsList(self):
        return self.events.as_list()

    def update(self, name=None, url=None, systemUrl=None):
        if name:
            self.name = name
        if url:
            self.url = url
        if systemUrl:
            self.systemUrl = systemUrl
            self.Connect()

    def __setstate__(self, sdict):
        taglist = sdict.pop('taglist', None) # old backup
        super(ClientInfo, self).__setstate__(sdict)
        if taglist:
            for tag in taglist:
                self.RegisterTagEvent(tag, ETagEvent.Set)

    def Resume(self):
        self.Connect()
        self.active = True

    def Suspend(self):
        self.active = False

    def SetVersion(self, version):
        logging.debug("set client '%s' version to %s" % (self.name, version))
        self.version = version

    def _DoSendEventsIfNeed(self):
        tosend = self.events[:self.MAX_TAGS_BULK]
        if not tosend:
            return

        logging.debug("SendData to %s: %d events", self.name, len(tosend))

        def send_as_events():
            try:
                self.connection.register_tags_events(tosend)
                return True
            except XMLRPCMethodNotSupported:
                self.SetVersion(1)
                return False

        def send_as_set_tags():
            tags = _get_tags_to_set(tosend)
            if not tags:
                return
            self.connection.set_tags(tags)

        if self.version < 2 or not send_as_events():
            send_as_set_tags()

        self.events.pop(len(tosend))

    def _SendEventsIfNeed(self):
        with self.send_events_lock: # ATW this lock is redundant
            self._DoSendEventsIfNeed()

    def _SendSubscriptionsIfNeed(self, local_server_network_name):
        tosend = list(self.subscriptions)[:self.MAX_TAGS_BULK]
        if not tosend:
            return

        logging.debug("SendData to %s: %d subscriptions", self.name, len(tosend))

        self.connection.register_share(tosend, local_server_network_name)
        self.subscriptions.difference_update(tosend)

    def TryUpdatePeerVersion(self):
        try:
            version = self.connection.get_client_version()
        except XMLRPCMethodNotSupported:
            self.SetVersion(1)
            return False
        self.SetVersion(version)
        return True

    def TrySendMyVersion(self, local_server_network_name):
        try:
            self.connection.set_client_version(local_server_network_name, PROTOCOL_VERSION)
        except XMLRPCMethodNotSupported:
            self.SetVersion(1)

    def _Communicate(self, f):
        self.Connect()

        try:
            f()

            self.errorsCnt = 0
            logging.debug("SendData to %s: ok", self.name)
        except (IOError, socket.timeout) as e:
            logging.warning("SendData to %s: failed: %s", self.name, e)
            self.lastError = e
            self.errorsCnt += 1
        except Exception as e:
            logging.error("SendData to %s: failed: %s", self.name, e)

    def SendDataIfNeed(self, local_server_network_name):
        if not self.subscriptions and not self.events:
            return

        def impl():
            if self.errorsCnt:
                self.TryUpdatePeerVersion()

            self._SendEventsIfNeed()
            self._SendSubscriptionsIfNeed(local_server_network_name)

        self._Communicate(impl)

    def TryInitializePeersVersions(self, local_server_network_name):
        def impl():
            self.TryUpdatePeerVersion()
            self.TrySendMyVersion(local_server_network_name)
        self._Communicate(impl)

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict.pop("connection", None)
        return getattr(super(ClientInfo, self), "__getstate__", lambda: sdict)()

    def __repr__(self):
        return "<ClientInfo %s alive: %r>" % (self.name, self.active)


class TopologyInfo(Unpickable(servers=dict, location=str)):
    def ReloadConfig(self, location=None):
        if location is not None:
            self.location = location
        self.Update(TopologyInfo.ReadConfig(self.location))

    @classmethod
    def ReadConfig(cls, location):
        if location.startswith("local://"):
            return cls.ReadConfigFromFile(location[8:])
        elif location.startswith("svn+ssh://"):
            return cls.ReadConfigFromSVN(location)
        raise AttributeError("unknown config location %s" % location)

    @classmethod
    def ReadConfigFromSVN(cls, location):
        tmp_dir = tempfile.mkdtemp(dir=".", prefix="network-topology")
        try:
            config_temporary_path = os.path.join(tmp_dir, os.path.split(location)[1])
            runproc.check_call(
                ["svn", "export", "--force", "--non-interactive", "-q", location, config_temporary_path])
            return cls.ReadConfigFromFile(config_temporary_path)
        finally:
            if os.path.isdir(tmp_dir):
                shutil.rmtree(tmp_dir)

    @classmethod
    def ReadConfigFromFile(cls, filename):
        configParser = ConfigParser()
        assert filename in configParser.read(filename), \
            "error in network topology file %s" % filename
        return configParser.items("servers")

    def Update(self, data):
        for k, v in data:
            server_info = map(lambda s: s.strip(), v.split(','))
            self.servers.setdefault(k, ClientInfo()).update(k, *server_info)

    def GetClient(self, hostname, checkname=True):
        if checkname and hostname not in self.servers:
            raise RuntimeError("unknown host '%s'" % hostname)
        return self.servers.get(hostname, None)

    def UpdateContext(self, context):
        self.location = context.network_topology

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["servers"] = self.servers.copy()
        return getattr(super(TopologyInfo, self), "__getstate__", lambda: sdict)()


class MapSetDB(object):
    def __init__(self, filename):
        self.db = bsddb3.btopen(filename, "c")
        #self.lock = fork_locking.Lock()

    def get(self, key):
        if self.db.has_key(key):
            data = self.db[key]
            return cPickle.loads(data)
        return set()

    def _modify(self, key, f):
        #with self.lock:
        if True: # locked in ConnectionManager
            values = self.get(key)
            if f(values):
                self.db[key] = cPickle.dumps(values)
                self.db.sync()

    def add(self, key, value):
        def impl(values):
            if value in values:
                return False
            values.add(value)
            return True
        self._modify(key, impl)

    def remove(self, key, value):
        def impl(values):
            if value not in values:
                return False
            values.discard(value)
            return True
        self._modify(key, impl)


class ConnectionManager(Unpickable(topologyInfo=TopologyInfo,
                                   scheduledTasks=TimedSet.create,
                                   lock=PickableLock,
                                   alive=(bool, False),
                                   tags_file=str),
                        ICallbackAcceptor):
    def InitXMLRPCServer(self):
        self.rpcserver = SimpleXMLRPCServer(("", self.port), allow_none=True)
        self.rpcserver.register_function(self.set_client_version, "set_client_version")
        self.rpcserver.register_function(self.get_client_version, "get_client_version")
        self.rpcserver.register_function(self.set_tags, "set_tags")
        self.rpcserver.register_function(self.register_tags_events, "register_tags_events")
        self.rpcserver.register_function(self.list_clients, "list_clients")
        self.rpcserver.register_function(self.list_tags, "list_tags")
        self.rpcserver.register_function(self.suspend_client, "suspend_client")
        self.rpcserver.register_function(self.resume_client, "resume_client")
        self.rpcserver.register_function(self.reload_config, "reload_config")
        self.rpcserver.register_function(self.register_share, "register_share")
        self.rpcserver.register_function(self.unregister_share, "unregister_share")
        self.rpcserver.register_function(self.get_client_info, "get_client_info")
        self.rpcserver.register_function(self.list_shares, "list_shares")
        self.rpcserver.register_function(self.list_shared_events, "list_shared_events")
        self.rpcserver.register_function(self.list_subscriptions, "list_subscriptions")
        self.rpcserver.register_function(self.check_connection, "check_connection")
        self.rpcserver.register_function(self.ping, "ping")

    def UpdateContext(self, context):
        self.scheduler = context.Scheduler
        self.network_name = context.network_name
        self.tags_file = context.remote_tags_db_file
        self.port = context.system_port
        if self.tags_file:
            self.acceptors = MapSetDB(self.tags_file)
        self.topologyInfo.UpdateContext(context)
        self.max_remotetags_resend_delay = context.max_remotetags_resend_delay

    def Start(self):
        if not self.network_name or not self.tags_file or not self.port:
            logging.warning("ConnectionManager could'n start: wrong configuration. " +
                            "network_name: %s, remote_tags_db_file: %s, system_port: %r",
                            self.network_name, self.tags_file, self.port)
            return

        self.ReloadConfig()
        logging.debug("after_reload_config")

        for client in self.topologyInfo.servers.values():
            if client.active and client.name != self.network_name:
                client.TryInitializePeersVersions(self.network_name)
        logging.debug("after_clients_versions_init")

        self.alive = True
        self.InitXMLRPCServer()
        self._accept_loop_thread = ProfiledThread(target=self.ServerLoop, name_prefix='ConnManager')
        self._accept_loop_thread.start()
        logging.debug("after_connection_manager_loop_start")

        for client in self.topologyInfo.servers.values():
            self.scheduler.ScheduleTaskT(0, self.SendData, client, skip_logging=True)

    def Stop(self):
        self.alive = False
        self._accept_loop_thread.join()
        self.rpcserver = None # shutdown listening socket

    def ServerLoop(self):
        rpc_fd = self.rpcserver.fileno()
        while self.alive:
            rout, _, _ = select.select((rpc_fd,), (), (), 0.01)
            if rpc_fd in rout:
                self.rpcserver.handle_request()

    def SendData(self, client):
        if self.alive and client.active:
            client.SendDataIfNeed(self.network_name)

        if hasattr(self, "scheduler"):
            self.scheduler.ScheduleTaskT(
                min(client.PENALTY_FACTOR ** client.errorsCnt, self.max_remotetags_resend_delay),
                self.SendData,
                client,
                skip_logging=True
            )

    def RegisterTagEvent(self, tag, event, message=None):
        if not isinstance(tag, TagBase):
            raise RuntimeError("%s is not Tag class instance", tag.GetName())
        if tag.IsRemote():
            return

        tagname = tag.GetName()
        with self.lock: # see register_share
            acceptors = self.acceptors.get(tagname)
            if acceptors:
                logging.debug("on %s connmanager %s with acceptors list %s", TagEventName[event], tagname, acceptors)
                for clientname in acceptors:
                    self.RegisterTagEventForClient(clientname, tagname, event, message)

    def RegisterTagEventForClient(self, clientname, tagname, event, message=None):
        logging.debug("%s remote tag %s on host %s", TagEventName[event], tagname, clientname)
        client = self.topologyInfo.GetClient(clientname, checkname=False)
        if client is None:
            logging.error("unknown client %s appeared", clientname)
            return False
        client.RegisterTagEvent("%s:%s" % (self.network_name, tagname), event, message)

    def ReloadConfig(self, filename=None):
        old_servers = set(self.topologyInfo.servers.keys())
        self.topologyInfo.ReloadConfig()
        new_servers = set(self.topologyInfo.servers.keys())
        new_servers -= old_servers
        if self.alive:
            for client in new_servers:
                self.scheduler.ScheduleTaskT(0, self.SendData, self.topologyInfo.servers[client], skip_logging=True)

    def Subscribe(self, tag):
        if tag.IsRemote():
            client = self.topologyInfo.GetClient(tag.GetRemoteHost(), checkname=True)
            client.Subscribe(tag.GetName())
            return True
        return False

    @traced_rpc_method()
    def set_tags(self, tags): # obsolete
        logging.debug("set %d remote tags", len(tags))
        for tagname in tags:
            self.scheduler.tagRef.AcquireTag(tagname).CheckRemote().Set()
        return True

    @traced_rpc_method()
    def set_client_version(self, clientname, version):
        self.topologyInfo.GetClient(clientname, checkname=True).SetVersion(int(version))
        logging.debug("set client version for %s to %s", clientname, version)
        return True

    @traced_rpc_method()
    def get_client_version(self):
        return PROTOCOL_VERSION

    @traced_rpc_method()
    def register_tags_events(self, updates):
        tagRef = self.scheduler.tagRef
        logging.debug("register_tags_events %d: %s", len(updates), updates)
        for update in updates:
            tagRef.AcquireTag(update[0]).CheckRemote().Modify(*update[1:])
            logging.debug("done with: %s", update)
        logging.debug("register_tags_events %d: done", len(updates))
        return True

    @traced_rpc_method()
    def list_clients(self):

        return [{"name": client.name,
                 "url": client.url,
                 "systemUrl": client.systemUrl,
                 "active": client.active,
                 "version": client.version,
                 "errorsCount": client.errorsCnt,
                 "tagsCount": len(client.events),
                 "subscriptionsCount": len(client.subscriptions),
                 "lastError": str(client.lastError)} for client in self.topologyInfo.servers.values()]

    @traced_rpc_method()
    def list_tags(self, name_prefix):
        data = set()
        for server in self.topologyInfo.servers.values():
            if name_prefix is None or server.name.startswith(name_prefix):
                data.update(server.GetEventsAsTagsToSet())
        return list(data)

    @traced_rpc_method()
    def suspend_client(self, name):
        client = self.topologyInfo.GetClient(name)
        return client.Suspend()

    @traced_rpc_method()
    def resume_client(self, name):
        client = self.topologyInfo.GetClient(name)
        return client.Resume()

    @traced_rpc_method()
    def reload_config(self, location=None):
        self.ReloadConfig(location)

    @traced_rpc_method()
    def register_share(self, tags, clientname):
        tagRef = self.scheduler.tagRef
        logging.debug("register_share %d tags for %s: %s", len(tags), clientname, tags)
        for tagname in tags:
            # XXX
            # 1. this lock only guarantee eventual-consistency of tag's history
            # 2. clients of self may see duplicates of events (even Reset)
            # 3. also guard self.acceptors
            with self.lock:
                self.acceptors.add(tagname, clientname)
                if tagRef._RawTag(tagname).IsLocallySet():
                    self.RegisterTagEventForClient(clientname, tagname, ETagEvent.Set)
        logging.debug("register_share %d tags for %s: done", len(tags), clientname)

    @traced_rpc_method()
    def unregister_share(self, tagname, clientname):
        with self.lock:
            return self.acceptors.remove(tagname, clientname)

    @traced_rpc_method()
    def get_client_info(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        res = {"name": client.name,
               "url": client.url,
               "systemUrl": client.systemUrl,
               "active": client.active,
               "version": client.version,
               "errorsCount": client.errorsCnt,
               "deferedTagsCount": len(client.events),
               "subscriptionsCount": len(client.subscriptions),
               "lastError": str(client.lastError)}
        return res

    @traced_rpc_method()
    def list_shares(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return _get_tags_to_set(client.GetEventsAsList())

    @traced_rpc_method()
    def list_shared_events(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return client.GetEventsAsList()

    @traced_rpc_method()
    def list_subscriptions(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return list(client.subscriptions)

    @traced_rpc_method()
    def check_connection(self, clientname):
        client = self.topologyInfo.GetClient(clientname)
        return client.connection.ping()

    @traced_rpc_method()
    def ping(self):
        return True

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["scheduledTasks"] = self.scheduledTasks.copy()
        sdict.pop("scheduler", None)
        sdict.pop("rpcserver", None)
        sdict.pop("acceptors", None)
        sdict.pop("_accept_loop_thread", None)
        sdict["alive"] = False
        return getattr(super(ConnectionManager, self), "__getstate__", lambda: sdict)()
