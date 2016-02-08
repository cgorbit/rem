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
import tempfile
import subprocess
import re

from common import *
from callbacks import TagBase, LocalTag, CloudTag, RemoteTag, CallbackHolder, ICallbackAcceptor, ETagEvent
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
        self._running = {}

    # TODO Backup state
    # TODO Write journal

    def _alloc_id(self):
        ret = self._next_id
        self._next_id += 1
        return ret

    def _on_done(self, id, f):
        with self._lock:
            if f.is_success():
                self._running.pop(id)
                return
            else:
                pass # TODO

        logging.warning("Task #%d failed %s" % (id, self._running[id]))

    def update(self, update):
        with self._lock:
            logging.debug("SafeCloud.update(%s)" % str(update))

            id = self._alloc_id()
            done = self._cloud.serial_update(update)
            self._running[id] = update

        # This call may lead to immediate _on_done in current thread
        done.subscribe(lambda f: self._on_done(id, f))

class TagReprModifier(object):
    _STOP_INDICATOR = object()

    def __init__(self, pool_size=100):
        self._connection_manager = None
        self._pool_size = pool_size
        self._queues  = [Queue() for _ in xrange(pool_size)]

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
        self._threads = []

        for idx in xrange(pool_size):
            q = self._queues[idx]
            t = ProfiledThread(target=self._worker, args=(q,), name_prefix='TagReprModifier')
            self._threads.append(t)

            t.start()

    def _process_update(self, update):
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

    def _worker(self, queue):
        while True:
            update, promise = queue.get()

            if update is self._STOP_INDICATOR:
                return

            self._process_update(update)

            # XXX see tofileOldItems
            del update

            if promise:
                promise.set()

class TagsMasks(object):
    @classmethod
    def _parse(cls, input):
        with_min_value = []
        without_min_value = []

        for lineno, line in enumerate(input, 1):
            regexp_str = None
            min_value = None
            fields = line.rstrip('\n').split('\t')

            if len(fields) == 1:
                regexp_str = fields[0]
            elif len(fields) == 2:
                regexp_str = fields[0]
                try:
                    min_value = int(fields[1])
                except ValueError as e:
                    raise ValueError("Bad min_value '%s' on line %d: %s" % (fields[1], lineno, e))

                if min_value < 0:
                    raise ValueError("min_value is negative on line %d" % lineno)
            else:
                raise ValueError("Malformed line %d" % lineno)

            try:
                regexp = re.compile(regexp_str)
            except Exception as e:
                raise ValueError("Malformed regexp '%s' on line %d: %s" % (regexp_str, lineno, e))

            if min_value is None:
                if regexp.groups:
                    raise ValueError("No min_value but regexp contains groups '%s' on line %d" % (regexp_str, lineno))
            elif regexp.groups != 1:
                raise ValueError("min_value without groups in regexp '%s' on line %d" % (regexp_str, lineno))

            if min_value is None:
                without_min_value.append(regexp)
            else:
                with_min_value.append((regexp, min_value))

        return (with_min_value, without_min_value)

    @classmethod
    def parse(cls, input):
        with_min_value, without_min_value = cls._parse(input)

        if not with_min_value and not without_min_value:
            return cls.get_empty_matcher()

        pattern = re.compile(
            '^(?:' \
            + '|'.join([r.pattern for r in [r for r, _ in with_min_value] + without_min_value]) \
            + ')$'
        )

        min_values = [v for _, v in with_min_value]

        def match(str):
            m = pattern.match(str)
            if not m:
                #print >>sys.stderr, '+ not matched at all'
                return False

            if m.lastindex is None:
                #print >>sys.stderr, '+ lastindex is None (without_min_value)'
                return True

            group_idx = m.lastindex - 1

            matched_group = m.groups()[group_idx]
            #print >>sys.stderr, '+ matched_group = %s' % matched_group
            try:
                val = int(matched_group)
            except ValueError as e:
                raise RuntimeError("Initial regexp match non integer '%s' in '%s'" % (matched_group, str))

            #print >>sys.stderr, '+ val = %d, min_value = %d' % (val, min_values[group_idx])
            return val >= min_values[group_idx]

        match.count = len(with_min_value) + len(without_min_value)

        match.regexps = [r.pattern for r in without_min_value] \
            + [(r.pattern, min_value) for r, min_value in with_min_value]

        return match

    @classmethod
    def load(cls, location):
        with tempfile.NamedTemporaryFile(prefix='cloud_tags_list') as file:
            subprocess.check_call(
                ["svn", "export", "--force", "-q", "--non-interactive", location, file.name])

            with open(file.name) as input:
                return cls.parse(input)

    @staticmethod
    def get_empty_matcher():
        ret = lambda name: False
        ret.count = 0
        ret.regexps = []
        return ret

class TagStorage(object):
    _CLOUD_TAG_REPR_UPDATE_WAITING_TTL = 7200 # Hack for hostA:RemoteTag -> hostB:CloudTag

    def __init__(self, rhs=None):
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
        self._match_cloud_tag = TagsMasks.get_empty_matcher()
        self._masks_reload_thread = None
        self._masks_should_stop = threading.Event()
        self._last_tag_mask_match_error_time = 0
        if rhs:
            if isinstance(rhs, dict):
                self.inmem_items = rhs
            elif isinstance(rhs, TagStorage):
                self.inmem_items = rhs.inmem_items
                self.infile_items = rhs.infile_items
                self.db_file = rhs.db_file

    def list_cloud_tags_masks(self):
        return self._match_cloud_tag.regexps

    def PreInit(self):
        if self._cloud_tags_server:
            try:
                self._match_cloud_tag = self._load_masks()
            except Exception as e:
                raise RuntimeError("Can't load cloud_tags_masks from %s: %s" % (self._cloud_tags_masks, e))

    def _load_masks(self):
        return TagsMasks.load(self._cloud_tags_masks)

    def Start(self):
        self.tag_logger.Start()
        self._repr_modifier.Start()

        if self._cloud_tags_server:
            self._masks_reload_thread = ProfiledThread(
                target=self._masks_reload_loop, name_prefix='TagsMasksReload')
            self._masks_reload_thread.start()

            self._cloud = cloud_client.getc(addr=self._cloud_tags_server, on_event=self._on_cloud_journal_event)
            self._safe_cloud = SafeCloud(self._cloud, self.tag_logger)
            self._subscribe_all()

    def _subscribe_all(self):
        with self.lock:
            # FIXME могут ли они быть ещё где-то?
            cloud_tags = set(
                tag.GetFullname()
                    for tag in self.inmem_items.itervalues()
                        if tag.IsCloud()
            )

            # TODO split in groups in cloud_client?

            if cloud_tags:
                self._cloud.subscribe(cloud_tags)

    def _on_cloud_journal_event(self, ev):
        logging.debug('before journal event %s' % ev)

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
            logging.debug('overtaking reset %s.%d.%d for %d' % (ev.tag_name, ev.version, ev.last_reset_version, tag.version))
            add_event(ETagEvent.Reset, ev.last_reset_version, ev.last_reset_comment) # TODO last_reset_comment is wrong

        add_event(ev.event, ev.version, ev.last_reset_comment if ev.event == ETagEvent.Reset else None)

        logging.debug('after journal event for %s' % ev.tag_name)

    def _masks_reload_loop(self):
        while True:
            if self._masks_should_stop.wait(self._cloud_tags_masks_reload_interval):
                return

            try:
                match = self._load_masks()
            except Exception as e:
                logging.error("Failed to reload tags' masks from: %s" % e)
                continue

            if self._match_cloud_tag.count and not match.count:
                logging.warning("New cloud tags masks discarded: old count %d, new count %d" % (
                    self._match_cloud_tag.count, match.count))
                continue

            logging.debug("Cloud tag's masks reloaded. Regexp count: %d" % match.count)
            self._match_cloud_tag = match

    def Stop(self):
        # TODO Kosher
        if self._cloud:
            self._cloud.stop(timeout=10)
            self._cloud.stop(wait=False)

        if self._masks_reload_thread:
            self._masks_should_stop.set()
            self._masks_reload_thread.join()

        #self._safe_cloud = None

        self._repr_modifier.Stop()
        self.tag_logger.Stop()

    def __reduce__(self):
        return TagStorage, (self.inmem_items.copy(), )

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
                    return state.IsLocallySet()
                else:
                    return state.is_set
            else:
                if not state:
                    return None
                elif isinstance(state, TagBase):
                    return {'is_set': state.IsLocallySet()}
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

    # For calls from REM guts
    def _modify_cloud_tag_safe(self, tag, event, msg=None):
        update = (tag.GetFullname(), event, msg)
        self._set_min_release_time(tag)
        self._safe_cloud.update(update)

    def _set_min_release_time(self, tag):
        tag._min_release_time = time.time() + self._CLOUD_TAG_REPR_UPDATE_WAITING_TTL

    # for calls from from RPC
    def _modify_tags_unsafe(self, updates):
        if not updates:
            return future.READY_FUTURE

        cloud_updates = []
        local_updates = []

        for update in updates:
            tag_name = update[0]

            if self.IsCloudTagName(tag_name):
                self._set_min_release_time(self.AcquireTag(tag_name).inner)
                cloud_updates.append(update)
            else:
                update = list(update)
                update[0] = self.AcquireTag(tag_name)
                local_updates.append(update)

        local_done = self._modify_local_tags(local_updates, with_future=True) if local_updates \
            else None

        cloud_done = self._cloud.update(cloud_updates) if cloud_updates \
            else None

        #futures = filter(None, [local_done, cloud_done])
        #return WaitFutures(futures) if len(futures) > 1 else futures[0]
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

    def _modify_local_tag_safe(self, tag, event, msg=None):
        self._modify_local_tags([(tag, event, msg)], with_future=False)

    def IsRemoteTagName(self, tagname):
        return ':' in tagname

    def AcquireTag(self, tagname):
        raw = self._RawTag(tagname)

        with self.lock:
            ret = self.inmem_items.setdefault(tagname, raw)

            if ret is raw and ret.IsCloud() and self._cloud: # FIXME "and self._cloud" is bullshit?
                logging.debug('AcquireTag.subscribe(%s)' % tagname)
                self._cloud.subscribe(tagname)

        return TagWrapper(ret)

    def IsCloudTagName(self, name):
        if self.IsRemoteTagName(name): # fuck
            return False

        try:
            return self._match_cloud_tag(name)
        except Exception as e:
            now = time.time()
            if now - self._last_tag_mask_match_error_time > 5:
                logging.error("Failed to match tag masks: %s" % e)
            self._last_tag_mask_match_error_time = now
            return False

    def _create_tag(self, name):
        if self.IsRemoteTagName(name):
            return RemoteTag(name, self._modify_local_tag_safe)
        elif self.IsCloudTagName(name):
            return CloudTag(name, self._modify_cloud_tag_safe)
        else:
            return LocalTag(name, self._modify_local_tag_safe)

    def vivify_tags(self, tags):
        for tag in tags:
            tag._request_modify = self._modify_cloud_tag_safe if tag.IsCloud() else self._modify_local_tag_safe

    #def _RawTagAsItMustBe(self, name): # XXX See tofileOldItems and ConnectionManager.register_share
        #if not name:
            #raise ValueError("False tag name")

        #tag = self.inmem_items.get(name)
        #if tag:
            #return tag

        #if self.IsCloudTagName(name):
            #return CloudTag(name, self._modify_cloud_tag_safe)

        #if not self.db_file_opened:
            #self.DBConnect()

        #serialized = self.infile_items.get(name, None)
        #if serialized:
            #tag = cPickle.loads(serialized)
            #self.vivify_tags([tag])
            #return tag

        #cls = RemoteTag if self.IsRemoteTagName(name) else LocalTag
        #return cls(name, self._modify_local_tag_safe)

    def _GetTagLocalState(self, name):
        return self._RawTag(name, dont_create=True)

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
                self.vivify_tags([tag])
            elif dont_create:
                return None
            else:
                tag = self._create_tag(tagname)

        return tag

    def ListTags(self, name_regex=None, prefix=None, memory_only=True):
        #raise NotImplementedError() # XXX
# TODO Only local tags
        for name, tag in self.inmem_items.items():
            if name and (not prefix or name.startswith(prefix)) \
                and (not name_regex or name_regex.match(name)):
                yield name, tag.IsLocallySet()
        if memory_only:
            return
        inner_db = bsddb3.btopen(self.db_file, "r")
        try:
            name, tagDescr = inner_db.set_location(prefix) if prefix else inner_db.first()
            while True:
                if prefix and not name.startswith(prefix):
                    break
                if not name_regex or name_regex.match(name):
                    yield name, cPickle.loads(tagDescr).IsLocallySet()
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

        if context.cloud_tags_server and not context.cloud_tags_masks:
            raise RuntimeError("No 'cloud_tags_masks' option in config")

        self._cloud_tags_server = context.cloud_tags_server
        self._cloud_tags_masks = context.cloud_tags_masks
        self._cloud_tags_masks_reload_interval = context.cloud_tags_masks_reload_interval

    def Restore(self, timestamp):
        self.tag_logger.Restore(timestamp, self)

    def ListDependentPackets(self, tag_name):
        return self._RawTag(tag_name).GetListenersIds()

    def tofileOldItems(self):
        old_tags = set()
        unsub_tags = set()

        now = time.time()

        for name, tag in self.inmem_items.items():
            if tag.GetListenersNumber() == 0 \
                and sys.getrefcount(tag) == 4 \
                and getattr(tag, '_min_release_time', 0) < now:

                if tag.IsCloud():
                    unsub_tags.add(name)

                # Hack for hostA:RemoteTag -> hostB:CloudTag
                # XXX Store old cloud tags to local DB too, so ConnectionManager.register_share
                # will work from the box with cloud tags that have gone from inmem_items
                old_tags.add(name)

        if not self.db_file_opened:
            with self.lock:
                self.DBConnect()

# XXX TODO At this point GetListenersNumber and getrefcount may change

        with self.lock:
            if unsub_tags:
                self._cloud.unsubscribe(unsub_tags)

            for name in old_tags:
                tag = self.inmem_items.pop(name)
                tag.callbacks.clear()
                tag.__dict__.pop('_min_release_time', None) # FIXME
                try:
                    self.infile_items[name] = cPickle.dumps(tag)
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
