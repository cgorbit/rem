#coding: utf-8
from __future__ import with_statement
import logging
import os
import sys
import time
import weakref
import bsddb3
import cPickle
import threading

from common import *
from callbacks import TagBase, LocalTag, CloudTag, RemoteTag, CallbackHolder, ICallbackAcceptor
from journal import TagLogger
from packet import PacketState, JobPacket
from Queue import Queue
import fork_locking
from future import Promise, WaitFutures
from profile import ProfiledThread
import cloud_client

__all__ = ["GlobalPacketStorage", "BinaryStorage", "ShortStorage", "TagStorage", "PacketNamesStorage", "MessageStorage"]


class GlobalPacketStorage(object):
    def __init__(self):
        self.box = weakref.WeakValueDictionary()
        self.iteritems = self.box.iteritems

    def add(self, pck):
        self.box[pck.id] = pck

    def update(self, list):
        map(self.add, list)

    def __getitem__(self, item):
        return self.box[item]

    def __setitem__(self, key, value):
        self.box[key] = value

    def keys(self):
        return self.box.keys()

    Add = add

    def GetPacket(self, id):
        return self.box.get(id)


class ShortStorage(Unpickable(packets=(TimedMap.create, {}),
                              lock=PickableLock)):
    PCK_LIFETIME = 1800

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["packets"] = sdict["packets"].copy()
        return getattr(super(ShortStorage, self), "__getstate__", lambda: sdict)()

    def forgetOldItems(self):
        barrierTm = time.time() - self.PCK_LIFETIME
        with self.lock:
            while len(self.packets) > 0:
                pck_id, (tm, pck) = self.packets.peak()
                if tm < barrierTm:
                    pck.ReleasePlace()
                    self.packets.pop(pck.id)
                else:
                    break

    def StorePacket(self, pck):
        with self.lock:
            self.packets.add(pck.id, pck)

    def GetPacket(self, id):
        with self.lock:
            idx = self.packets.revIndex.get(id, None)
            if idx is not None:
                return self.packets.values[idx][1]

    def PickPacket(self, id):
        with self.lock:
            return self.packets.pop(id)[1][1]


class BinaryStorage(Unpickable(files=dict, lifeTime=(int, 3600), binDirectory=str)):
    digest_length = 32

    def __init__(self):
        getattr(super(BinaryStorage, self), "__init__")()

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict["files"] = sdict["files"].copy()
        return getattr(super(BinaryStorage, self), "__getstate__", lambda: sdict)()

    @classmethod
    def create(cls, o=None):
        if o is None:
            return cls()
        if isinstance(o, BinaryStorage):
            return o

    def forgetOldItems(self):
        curTime = time.time()
        bad_files = set()
        for checksum, file in self.files.items():
            if file.LinksCount() == 0 and curTime - file.accessTime > self.lifeTime:
                bad_files.add(checksum)
        for checksum in bad_files:
            self.files.pop(checksum).release()

    def UpdateContext(self, context):
        for file in self.files.itervalues():
            file.FixLinks()
        if self.binDirectory != context.binary_directory:
            badFiles = set()
            for checksum, file in self.files.iteritems():
                estimated_path = os.path.join(context.binary_directory, checksum)
                if not os.path.isfile(estimated_path):
                    #if os.path.isfile(file.path):
                    if False:
                        shutil.move(file.path, estimated_path)
                    elif file.LinksCount() > 0:
                        print file.links
                        raise RuntimeError("binstorage\tcan't recover file %r" % file.path)
                    else:
                        badFiles.add(checksum)
                file.Relink(estimated_path)
            if badFiles:
                for checksum in badFiles:
                    del self.files[checksum]
                    logging.warning("binstorage\tnonexisted file %s cleaning attempt", checksum)
                logging.warning("can't recover %d files; %d files left in storage", len(badFiles), len(self.files))
        self.binDirectory = context.binary_directory
        self.lifeTime = context.binary_lifetime

    def GetFileByHash(self, checksum):
        file = self.files.get(checksum, None)
        return file

    def RegisterFile(self, fileObject):
        return self.files.setdefault(fileObject.checksum, fileObject)

    def CreateFile(self, data):
        return self.RegisterFile(BinaryFile.createFile(self.binDirectory, data))

    def CreateFileLocal(self, path, checksum):
        tmpfile = None
        try:
            if not os.path.isfile(path) or not os.access(path, os.R_OK):
                return False

            fd, tmpfile = tempfile.mkstemp(dir=self.binDirectory)
            os.close(fd)
            shutil.copy2(path, tmpfile)

            fileChecksum = BinaryFile.calcFileChecksum(tmpfile)
            if fileChecksum != checksum:
                return False

            remPath = os.path.join(self.binDirectory, checksum)
            os.rename(tmpfile, remPath)
            tmpfile = None
            self.RegisterFile(BinaryFile(remPath, checksum, True))
        finally:
            if tmpfile is not None:
                os.unlink(tmpfile)
        return True

    def HasBinary(self, checksum):
        file = self.GetFileByHash(checksum)
        return file and os.path.isfile(file.path)


class TagWrapper(object):
    __slots__ = ["inner", "__reduce_ex__"]

    def __init__(self, tag):
        self.inner = tag

    def __reduce_ex__(self, proto):
        return TagWrapper, (self.inner, )

    def __getattribute__(self, attr):
        if attr in TagWrapper.__slots__:
            return object.__getattribute__(self, attr)
        return self.inner.__getattribute__(attr)

class SafeCloud(object):
    def __init__(self, cloud, journal):
        self._next_id = 1
        self._cloud = cloud
        self._journal = journal
        self._lock = fork_locking.Lock()

    def _alloc_id(self):
        ret = self._next_id
        self._next_id += 1
        return ret

    def _on_done(self, id, f):
        with self._lock:
            if f.is_success():
                self._running.pop(id)
                return
        # TODO
        logging.warning("Task #%d failed %s" % (id, self._running[id]))

    def update(self, event):
        with self._lock:
            # XXX log_cloud_request and seq_update under lock, so sequential
            #self._journal.log_cloud_request(event) # TODO
            id = self._alloc_id()
            done = self._cloud.serial_update(event)
            self._running[id] = event

        # This call may lead to immediate _on_done in current thread
        done.subscribe(lambda f: self._on_done(id, f))

#class TagReprUpdate(object):
    #__slots__ = ['tag', 'event', 'msg', 'version']

    #def __init__(self, tag, event, msg=None, version=None):
        #self.tag = tag
        #self.event = event
        #self.msg = msg
        #self.version = version

class TagReprModifier(object):
    _STOP_INDICATOR = object()

    def __init__(self, pool_size=100):
        self._connection_manager = None
        self._pool_size = pool_size

    def Start(self):
        self._create_workers(self._pool_size)

    def UpdateContext(self, ctx):
        self._connection_manager = ctx.Scheduler.connManager

    def add(self, update, with_future=False):
        queue = self._queues[hash(update[0].GetFullname()) % len(self._queues)]
        promise = Promise() if with_future else None
        queue.put((update, promise))
        if promise:
            return promise.to_future()

    def Stop(self):
        for q in self._queues:
            q.put((self._STOP_INDICATOR, None))

        for t in self._threads:
            t.join()

    def _create_workers(self, pool_size):
        self._queues  = []
        self._threads = []

        for _ in xrange(pool_size):
            q = Queue()
            t = ProfiledThread(target=self._worker, args=(q,), name_prefix='TagReprModifier')
            self._queues.append(q)
            self._threads.append(t)

            t.start()

    def _worker(self, queue):
        while True:
            update, promise = queue.get()

            if update is self._STOP_INDICATOR:
                return

            tag, event = update[0:2]
            msg = None
            version = None
            if len(update) >= 3:
                msg = update[2]
            if len(update) >= 4:
                version = update[3]

            tag._ModifyLocalState(event, msg, version) # FIXME try/except?

        # FIXME try/except? NO!
            self._connection_manager.RegisterTagEvent(tag, event, msg)

            if promise:
                promise.set()


class TagStorage(object):
    def __init__(self, *args):
        self.lock = PickableLock()
        self.inmem_items = {}
        self.infile_items = None
        self.db_file = ""
        self.db_file_opened = False
        self._local_tag_modify_lock = threading.Lock()
        self._repr_modifier = TagReprModifier() # TODO pool_size from rem.cfg
        self.tag_logger = TagLogger()
        self._cloud = None
        self._safe_cloud = None
        if len(args) == 1:
            if isinstance(args[0], dict):
                self.inmem_items = args[0]
            elif isinstance(args[0], TagStorage):
                self.inmem_items = args[0].inmem_items
                self.infile_items = args[0].infile_items
                self.db_file = args[0].db_file

    def Start(self):
        self.tag_logger.Start()
        self._repr_modifier.Start()
        self._cloud = cloud_client.getc(self._on_cloud_journal_event)
        self._safe_cloud = SafeCloud(self._cloud, self.tag_logger)
        self._subscribe_all()

    def _subscribe_all(self):
        with self.lock:
            # FIXME могут ли они быть ещё где-то, суки?
            cloud_tags = set(
                tag.GetFullname()
                    for tag in self.inmem_items.itervalues()
                        if tag.IsCloud()
            )

            # TODO split in groups in cloud_client?

            if cloud_tags:
                self._cloud.subscribe(cloud_tags)

    def _on_cloud_journal_event(self, ev):
        logging.debug('before journal event for %s' % ev.tag_name)

        with self.lock: # FIXME
            tag = self.inmem_items.get(ev.tag_name)

        if not tag:
            logging.warning('no object in inmem_items for cloud tag %s' % ev.tag_name)
            return

        if not tag.IsCloud(): # it's like assert
            logging.error('tag %s is not cloud tag in inmem_items but receives event from cloud' % ev.tag_name)
            return

# TODO user flag in cloud_client.subscribe() that will passed back _on_cloud_journal_event
        if tag.version >= ev.version:
            logging.warning('local version (%d) >= journal version (%d) for tag %s' \
                % (tag.version, ev.version, ev.tag_name))
            return

        def add_event(event, version, msg=None):
            self._repr_modifier.add((tag, event, msg, version))

    # FIXME here with warning, on state sync without it
        if ev.version > ev.last_reset_version and tag.version < ev.last_reset_version:
            add_event(ETagEvent.Reset, ev.last_reset_version, ev.last_reset_comment)

        add_event(ev.event, ev.version, ev.comment)

        logging.debug('after journal event for %s' % ev.tag_name)

    def Stop(self):
        # TODO Kosher
        self._cloud.stop(timeout=10)
        self._cloud.stop(wait=False)

# FIXME order
        self._safe_cloud = None # TODO

        self._repr_modifier.Stop()
        self.tag_logger.Stop()

    def __reduce__(self):
        return TagStorage, (self.inmem_items.copy(), )

######
    def _lookup_tags(self, tags):
        return self.__lookup_tags(tags, False)

    def _are_tags_set(self, tags):
        return self.__lookup_tags(tags, True)

    def __lookup_tags(self, tags, as_bools):
        ret = {}

        cloud_tags = set()

    # FIXME not as closure
        def _ret_value(state):
            if as_bools:
                if not state:
                    return False
                elif isinstance(state, TagBase):
                    return state.IsSet()
                else:
                    return state.is_set
            else:
                if not state:
                    return None
                elif isinstance(state, TagBase):
                    return {'is_set': state.IsSet()}
                else:
                    return state.__dict__

        for tag in tags:
            if self.IsCloudTagName(tag):
                cloud_tags.add(tag)
            else:
                ret[tag] = _ret_value(self._RawTag(tag, dont_create=True))

        promise = Promise()

        if not cloud_tags:
            promise.set(ret)
            return promise.to_future()

        cloud_done = self._cloud.lookup(cloud_tags)

        def on_cloud_done(f):
            if f.is_success():
                cloud_result = f.get()
                for tag in cloud_tags:
                    ret[tag] = _ret_value(cloud_result.get(tag, None))
                promise.set(ret)
            else:
                promise.set(None, f.get_raw()[1]) # FIXME backtrace

        cloud_done.subscribe(on_cloud_done)

        return promise.to_future()

######
    def _modify_cloud_tag(self, safe, tag, event, msg=None):
        update = (tag.GetFullname(), event, msg)
        if safe:
            # For calls from REM guts
            self._safe_cloud.update(update)
        else:
            # For calls from RPC
            return self._cloud.update([update]) # future

    # from RPC
    def _modify_tags_unsafe(self, updates):
        if not updates:
            return future.READY_FUTURE

        cloud_updates = []
        local_updates = []

        for update in updates:
            if self.IsCloudTagName(update[0]):
                cloud_updates.append(update)
            else:
                #if isinstance(update[0], str):
                if True:
                    update = list(update)
                    update[0] = self.AcquireTag(update[0])
                local_updates.append(update)

        local_done = self._modify_local_tags(local_updates, with_future=True) if local_updates \
            else None

        cloud_done = self._cloud.update(cloud_updates) if cloud_updates \
            else None

        if local_done is None:
            return cloud_done
        elif cloud_done is None:
            return local_done
        else:
            return WaitFutures([cloud_done, local_done])

    def _modify_tag_unsafe(self, tagname, event, msg=None):
        return self._modify_tags_unsafe([(tagname, event, msg)]) # FIXME own faster impl?

    def _modify_local_tags(self, updates, with_future=False):
        done = []

        with self._local_tag_modify_lock: # FIXME
            for update in updates:
                self.tag_logger.LogEvent(*update)
                done.append(self._repr_modifier.add(update, with_future))

        if not with_future:
            return

        return done[0] if len(done) == 1 else WaitFutures(done)

    def _modify_local_tag(self, safe, tag, event, msg=None):
        with_future = not safe
        return self._modify_local_tags([(tag, event, msg)], with_future)
######

# TODO check usage

    #def SetTag(self, tagname):
        #self.AcquireTag(tagname).Set()

    #def UnsetTag(self, tagname):
        #self.AcquireTag(tagname).Unset()

    #def ResetTag(self, tagname, message):
        #self.AcquireTag(tagname).Reset(message)

    #def CheckTag(self, tagname):
        #return self._RawTag(tagname).IsSet()

######

    def IsRemoteTagName(self, tagname):
        return ':' in tagname

    def AcquireTag(self, tagname):
        tag = self._RawTag(tagname)

        with self.lock:
            ret = self.inmem_items.setdefault(tagname, tag)

            if ret is tag and tag.IsCloud() and self._cloud: # FIXME "and self._cloud" is bullshit?
                logging.debug('AcquireTag.subscribe(%s)' % tagname)
                self._cloud.subscribe([tagname])

            return TagWrapper(ret)

    def TryGetInMemoryTag(self, name):
        with self.lock: # FIXME useless lock
            obj = self.inmem_items.get(name)
            return TagWrapper(obj) if obj else None # FIXME WHAT TagWrapper FOR?!??!?

    def IsCloudTagName(self, name):
        return name.startswith('_cloud_') or False # TODO

    def _create_tag(self, name):
        if self.IsRemoteTagName(name):
            return RemoteTag(name, self._modify_local_tag)
        elif self.IsCloudTagName(name):
            return CloudTag(name, self._modify_cloud_tag)
        else:
            return LocalTag(name, self._modify_local_tag)

    def vivify_tags(self, tags):
        for tag in tags:
            tag._request_modify = self._modify_cloud_tag if tag.IsCloud() else self._modify_local_tag

    def _RawTag(self, tagname, dont_create=False):
        if not tagname:
            raise ValueError("Bad tag name")

        tag = self.inmem_items.get(tagname, None)

        if tag is None:
            if not self.db_file_opened:
                self.DBConnect()
            tagDescr = self.infile_items.get(tagname, None)
            if tagDescr:
                tag = cPickle.loads(tagDescr)
            elif dont_create:
                return None
            else:
                tag = self._create_tag(tagname)

# XXX TODO Ignore in backup

        #for obj in self.additional_listeners:
            #tag.AddNonpersistentCallbackListener(obj)

        return tag

    def ListTags(self, name_regex=None, prefix=None, memory_only=True):
        #raise NotImplementedError() # XXX
# TODO Only local tags
        for name, tag in self.inmem_items.items():
            if name and (not prefix or name.startswith(prefix)) \
                and (not name_regex or name_regex.match(name)):
                yield name, tag.IsSet()
        if memory_only:
            return
        inner_db = bsddb3.btopen(self.db_file, "r")
        try:
            name, tagDescr = inner_db.set_location(prefix) if prefix else inner_db.first()
            while True:
                if prefix and not name.startswith(prefix):
                    break
                if not name_regex or name_regex.match(name):
                    yield name, cPickle.loads(tagDescr).IsSet()
                name, tagDescr = inner_db.next()
        except bsddb3._pybsddb.DBNotFoundError:
            pass
        inner_db.close()

    def DBConnect(self):
        self.infile_items = bsddb3.btopen(self.db_file, "c")
        self.db_file_opened = True

    def UpdateContext(self, context):
        self.db_file = context.tags_db_file
        self.DBConnect()
        self.conn_manager = context.Scheduler.connManager
        self.tag_logger.UpdateContext(context)
        self._repr_modifier.UpdateContext(context)

        #self.additional_listeners = set()
        #self.additional_listeners.add(context.Scheduler.connManager)
        #self.additional_listeners.add(self.tag_logger)

    def Restore(self, timestamp):
        self.tag_logger.Restore(timestamp, self)

    def ListDependentPackets(self, tag_name):
        return self._RawTag(tag_name).GetListenersIds()

    def tofileOldItems(self):
        old_tags = set()
        unsub_tags = set()

        for name, tag in self.inmem_items.items():
            #tag for removing have no listeners and have no external links for himself (actualy 4 links)
            if tag.GetListenersNumber() == 0 and sys.getrefcount(tag) == 4:
                if tag.IsCloud():
                    unsub_tags.add(name)
                else:
                    old_tags.add(name)

        if not self.db_file_opened:
            with self.lock:
                self.DBConnect()

# XXX FIXME At this point GetListenersNumber and getrefcount may change
        with self.lock:
            if unsub_tags:
                self._cloud.unsubscribe(unsub_tags)

            for name in old_tags:
                tag = self.inmem_items.pop(name)
                tag.callbacks.clear()
                serialized = cPickle.dumps(tag)
                try:
                    #self.infile_items[name] = cPickle.dumps(tag)
                    self.infile_items[name] = serialized
                except bsddb3.error as e:
                    if 'BSDDB object has already been closed' in e.message:
                        self.db_file_opened = False
                        self.db_file = None
                    raise
            self.infile_items.sync()


class PacketNamesStorage(ICallbackAcceptor):
    def __init__(self, *args, **kwargs):
        self.names = set(kwargs.get('names_list', []))
        self.lock = fork_locking.Lock()

    def __getstate__(self):
        return {}

    def Add(self, pck_name):
        with self.lock:
            self.names.add(pck_name)

    def Update(self, names_list=None):
        with self.lock:
            self.names.update(names_list or [])

    def Exist(self, pck_name):
        return pck_name in self.names

    def Delete(self, pck_name):
        with self.lock:
            if pck_name in self.names:
                self.names.remove(pck_name)

    def OnChange(self, packet_ref):
        if isinstance(packet_ref, JobPacket) and packet_ref.state == PacketState.HISTORIED:
            self.Delete(packet_ref.name)

    def OnJobDone(self, job_ref):
        pass

    def OnJobGet(self, job_ref):
        pass

    def OnPacketReinitRequest(self, pck):
        pass

class MessageStorage(object):
    pass
