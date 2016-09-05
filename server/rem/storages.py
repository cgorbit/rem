#coding: utf-8
from __future__ import with_statement
import os
import sys
import time
import weakref
import bsddb3
import cPickle
import threading
import tempfile
import re
import shutil

from common import Unpickable, TimedMap, PickableLock, cleanup_directory, BinaryFile, split_in_groups
import common as rem_common
from callbacks import TagBase, LocalTag, CloudTag, RemoteTag, CallbackHolder, ICallbackAcceptor, ETagEvent
from journal import TagLogger
from Queue import Queue
import fork_locking
from future import Promise, READY_FUTURE, CheckAllFuturesSucceed
from profile import ProfiledThread
from rem_logging import logger as logging

__all__ = ["GlobalPacketStorage", "BinaryStorage", "ShortStorage", "TagStorage", "MessageStorage"]


class OnDiskTagsConvertParams(object):
    def __init__(self, db_filename, in_memory_tags, cloud_tags_server):
        self.db_filename = db_filename
        self.in_memory_tags = in_memory_tags
        self.cloud_tags_server = cloud_tags_server

    def __repr__(self):
        return "<%s db=%s, len(in_mem_tags)=%d, proxy=%s>" \
            % (type(self).__name__,
               self.db_filename,
               len(self.in_memory_tags),
               self.cloud_tags_server)


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

    def ids(self):
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
                # Race with RPC
                pck_id, (tm, pck) = self.packets.peak()
                if tm < barrierTm:
                    pck.destroy()
                    self.packets.pop(pck.id)
                else:
                    break

    def StorePacket(self, pck):
        with self.lock:
            self.packets.add(pck.id, pck)

    def GetPacket(self, id):
        with self.lock:
            descr = self.packets.get(id)
            if descr:
                return descr[1]

    def PickPacket(self, id):
        with self.lock:
            pck = self.packets.pop(id)
            if pck:
                return pck[1][1]

    def ids(self):
        return self.packets.keys()


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

    def cleanup_fs(self):
        cleanup_directory(self.binDirectory, set(self.files.keys()))

    def _move_files(self, new_binary_directory):
        assert self.binDirectory != new_binary_directory
        if True:
            badFiles = set()
            for checksum, file in self.files.iteritems():
                estimated_path = os.path.join(new_binary_directory, checksum)
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

    def UpdateContext(self, context):
        if self.binDirectory != context.binary_directory:
            self._move_files(context.binary_directory)

        if context.fix_bin_links_at_startup:
            for file in self.files.itervalues():
                file.FixLinks()

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


class EditableState(object):
    def __init__(self, updates):
        by_id = {}
        for idx, (id, update) in enumerate(updates):
            by_id[id] = (idx, update)

        self._next_idx = len(updates)
        self._by_id = by_id

    # ATW Теоретически может возникнуть ситуация, когда нет полного
    # журнала от момента откладывания бэкапа с которого мы хотим загрузиться:
    # 1. нам пришлось отбросить свежие бэкапы (по каким-то причинам)
    # 2. нет в наличии всех частей журнала от момента откладывания
    #    выбранного нами бэкапа

    def start_request(self, id, update):
        if id not in self._by_id:
            self._by_id[id] = (self._next_idx, update)
            self._next_idx += 1

    def finish_request(self, id):
        self._by_id.pop(id, None)

    def get_result(self):
        return [
            (id, update)
                for id, (idx, update) in \
                    sorted(self._by_id.iteritems(), key=lambda (id, (idx, update)): idx)
        ]


class SerialUpdateOnlyDummyCloudClient(object):
    """This class allows SafeCloud to work correctly"""

    def __init__(self, wrong_setup_exc):
        self._wrong_setup_future = Promise().set(None, wrong_setup_exc).to_future()
        self._serial_update_promise = Promise()
        self._serial_update_future = self._serial_update_promise.to_future()

    def stop(self, timeout=None):
        self._serial_update_promise.set(None, RuntimeError("cloud client stopped"))

    def is_stopped(self):
        return self._serial_update_future.is_set()

    def wait_outgoing_empty(self):
        raise NotImplementedError()

    def serial_update(self, update):
        return self._serial_update_future

    def debug_server(self):
        return self._wrong_setup_future

    def subscribe(self, tags, with_future=True):
        return READY_FUTURE

    def unsubscribe(self, tags, with_future=True):
        return READY_FUTURE

    def lookup(self, tags):
        return self._wrong_setup_future

    def match(self, prefix=None, regexp=None, limit=None):
        return self._wrong_setup_future

    def update(self, updates):
        return self._wrong_setup_future


class SafeCloud(object):
    class NoopJournal(object):
        def log_cloud_request_start(*args):
            pass

        def log_cloud_request_finish(*args):
            pass

    def __init__(self, cloud, journal, prev_state):
        self._cloud = cloud
        self._uid_epoch = time.time() # FIXME Suppose that this is unique
        self._next_id = 1
        self._next_idx = 0
        self._lock = fork_locking.Lock()
        self._running_empty = fork_locking.Condition(self._lock)
        self._running = {}
        self._failed = []

        self._journal = self.NoopJournal()

        for id, update in prev_state.get_result():
            self._update(update, id=id)

        self._journal = journal

    def __getstate__(self):
        raise NotImplementedError()

    def _alloc_id(self):
        id = self._next_id
        self._next_id += 1
        return (self._uid_epoch, id)

    @classmethod
    def get_empty_state(cls):
        return EditableState([])

    def get_state_updates(self):
        with self._lock:
            items = [(idx, (id, update)) for id, (idx, update) in self._running.iteritems()]
            items.extend(self._failed)
        return [(id, update) for idx, (id, update) in sorted(items, key=lambda (idx, _): idx)]

    def get_state(self):
        return EditableState(self.get_state_updates())

    def wait_running_empty(self):
        with self._lock:
            if not self._running:
                return
            self._running_empty.wait()

    def _on_done(self, id, result):
        with self._lock:
            idx, update = self._running.pop(id)

            if result.is_success():
                self._journal.log_cloud_request_finish(id)
            else:
                #if not self._cloud.is_stopped():
                    #logging.error('cloud client not stopped, but update failed: %s' % str(update))

                self._failed.append((idx, (id, update)))

            if not self._running:
                self._running_empty.notify_all()

    def _update(self, update, id=None):
        with self._lock:
            logging.debug("SafeCloud.update(%s)" % str(update)) # until i will trust the code

            idx = self._next_idx
            self._next_idx += 1

            id = id or self._alloc_id()

            self._journal.log_cloud_request_start(id, update)

            done = self._cloud.serial_update(update)

            # FIXME
            assert id not in self._running

            self._running[id] = (idx, update)

        # This call may lead to immediate _on_done in current thread
        done.subscribe(lambda f: self._on_done(id, f))

    def update(self, update):
        self._update(update)


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

        try:
            tag._ModifyLocalState(event, msg, version)
        except Exception:
            logging.exception("Failed to process tag update %s" % (update,))

        if self._connection_manager:
            try:
                self._connection_manager.RegisterTagEvent(tag, event, msg)
            except Exception:
                logging.exception("Failed to pass update to ConnectionManager %s" % (update,))

    def _worker(self, queue):
        while True:
            update, promise = queue.get()

            if update is self._STOP_INDICATOR:
                return

            self._process_update(update)

            del update # XXX see tofileOldItems

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
        if location.startswith('file://'):
            return cls._load_from_file(location[7:])
        return cls._load_from_svn(location)

    @classmethod
    def _load_from_file(cls, path):
        with open(path) as input:
            return cls.parse(input)

    @classmethod
    def _load_from_svn(cls, path):
        with tempfile.NamedTemporaryFile(prefix='cloud_tags_list') as file:
            rem_common.proc_runner.check_call(
                ["svn", "export", "--force", "-q", "--non-interactive", path, file.name])
            return cls._load_from_file(file.name)

    @staticmethod
    def get_empty_matcher():
        ret = lambda name: False
        ret.count = 0
        ret.regexps = []
        return ret


class TagStorage(object):
    CLOUD_CLIENT_STOP_TIMEOUT = 10.0

    def __init__(self, rhs=None):
        self.lock = PickableLock()
        self.inmem_items = {}
        self.infile_items = None
        self.db_file = ""
        self.db_file_opened = False
        self._local_tag_modify_lock = threading.Lock()
        self._repr_modifier = TagReprModifier() # TODO pool_size from rem.cfg
        self._journal = TagLogger()
        self._cloud = None
        self._safe_cloud = None
        self._prev_safe_cloud_state = SafeCloud.get_empty_state()
        self._match_cloud_tag = TagsMasks.get_empty_matcher()
        self._masks_reload_thread = None
        self._masks_should_stop = threading.Event()
        self._last_tag_mask_error_report_time = 0
        self._all_tags_in_cloud = None
        if rhs:
            if isinstance(rhs, dict):
                self.inmem_items = rhs

            elif isinstance(rhs, TagStorage):
                self.inmem_items = rhs.inmem_items
                if hasattr(rhs, '_prev_safe_cloud_state'):
                    self._prev_safe_cloud_state = rhs._prev_safe_cloud_state

    def list_cloud_tags_masks(self):
        return self._match_cloud_tag.regexps

    def PreInit(self):
        if self._cloud_tags_server:
            self._pre_init_cloud_tags_setup()

    def _pre_init_cloud_tags_setup(self):
        # Allow to run REM without python-protobuf
        global cloud_client
        import cloud_client

        global cloud_connection
        import cloud_connection

        if self._cloud_tags_masks:
            self._do_initial_cloud_tags_masks_load()

        self._cloud_tags_server_instances \
            = cloud_connection.from_description(self._cloud_tags_server)

    def _do_initial_cloud_tags_masks_load(self):
        try_count = 3
        for idx in range(try_count):
            try:
                self._match_cloud_tag = self._load_masks()
            except Exception as e:
                logging.warning("Iter #%d of cloud tags masks (%s) loading failed: %s" % (
                    idx + 1, self._cloud_tags_masks, e))
            else:
                break

            if idx != try_count - 1:
                time.sleep(5)
        else:
            raise RuntimeError("Failed to load cloud tags masks for %d attempts" % try_count)

    def _load_masks(self):
        return TagsMasks.load(self._cloud_tags_masks)

    def Start(self):
        self._journal.Start()
        logging.debug("after_journal_start")

        self._repr_modifier.Start()
        logging.debug("after_repr_modifier_start")

        if self._cloud_tags_server:
            if self._cloud_tags_masks:
                self._masks_reload_thread = ProfiledThread(
                    target=self._masks_reload_loop, name_prefix='TagsMasksReload')
                self._masks_reload_thread.start()
                logging.debug("after_masks_reload_thread_start")

            self._cloud = self._create_cloud_client(self._on_cloud_journal_event)
        else:
            # For CloudTag's in tags.db
            self._cloud = SerialUpdateOnlyDummyCloudClient(
                RuntimeError("Wrong setup for cloud tags in rem-server"))

        self._create_safe_cloud()
        logging.debug("after_safe_cloud_start")

        self._subscribe_all()
        logging.debug("after_subscribe_all")

    def _create_cloud_client(self, on_event):
        if not self._has_cloud_setup():
            raise RuntimeError("No cloud tags setup")

        return cloud_client.Client(self._cloud_tags_server_instances, on_event=on_event)

    def _create_safe_cloud(self):
        self._safe_cloud = SafeCloud(self._cloud, self._journal, self._prev_safe_cloud_state)
        self._prev_safe_cloud_state = None

    def _subscribe_all(self):
        with self.lock:
            cloud_tags = set(
                tag.GetFullname()
                    for tag in self.inmem_items.itervalues()
                        if tag.IsCloud()
            )

            if cloud_tags:
                self._cloud.subscribe(cloud_tags, with_future=False)

    def _on_cloud_journal_event(self, ev):
        #logging.debug('before journal event %s' % ev)

        with self.lock:
            tag = self.inmem_items.get(ev.tag_name)

        if not tag:
            logging.warning('no object in inmem_items for cloud tag %s' % ev.tag_name)
            return

        if not tag.IsCloud(): # it's like assert
            logging.error('tag %s is not cloud tag in inmem_items but receives event from cloud' % ev.tag_name)
            return

        if tag.version >= ev.version:
            # TODO warn even on equal versions, but not for initial _subscribe_all
            if tag.version > ev.version:
                logging.warning('local version (%d) > journal version (%d) for tag %s' \
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
        self._cloud.stop(timeout=self.CLOUD_CLIENT_STOP_TIMEOUT)
        self._safe_cloud.wait_running_empty()

        if self._masks_reload_thread:
            self._masks_should_stop.set()
            self._masks_reload_thread.join()

        self._repr_modifier.Stop()
        self._journal.Stop()

    def __getstate__(self):
        return {
            'inmem_items': self.inmem_items.copy(),
            '_prev_safe_cloud_state': self._safe_cloud.get_state() if self._safe_cloud \
                                        else SafeCloud.get_empty_state()
        }

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
            # FIXME Consider that tag may exists as LocalTag in infile_items or inmem_items?
            if self._is_cloud_tag_name(tag):
                cloud_tags.add(tag)
            else:
                # dont_create=True to distinguish unset tags from non-existed
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
                promise.set(None, f.get_exception())

        cloud_done.subscribe(on_cloud_done)

        return promise.to_future()

    # For calls from REM guts
    def _modify_cloud_tag_safe(self, tag, event, msg=None):
        update = (tag.GetFullname(), event, msg)
        self._set_min_release_time(tag)
        self._safe_cloud.update(update)

    # Hack for hostA:RemoteTag -> hostB:CloudTag (shame on me)
    def _set_min_release_time(self, tag):
        tag._min_release_time = time.time() + self._cloud_tags_release_delay

    # for calls from from RPC
    def _modify_tags_unsafe(self, updates):
        if not updates:
            return READY_FUTURE

        cloud_updates = []
        local_updates = []

        for update in updates:
            tag_name = update[0]

            tag = self.AcquireTag(tag_name)

            if tag.IsCloud():
                self._set_min_release_time(tag.inner)
                cloud_updates.append(update)
            else:
                update = list(update)
                update[0] = tag
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
            return CheckAllFuturesSucceed([cloud_done, local_done])

    def _modify_tag_unsafe(self, tagname, event, msg=None):
        return self._modify_tags_unsafe([(tagname, event, msg)]) # FIXME own faster impl?

    def _modify_local_tags(self, updates, with_future=False):
        done = []

        with self._local_tag_modify_lock: # FIXME
            for update in updates:
                self._journal.log_local_tag_event(*update)
                done.append(self._repr_modifier.add(update, with_future))

        if not with_future:
            return

        return done[0] if len(done) == 1 else CheckAllFuturesSucceed(done)

    def _modify_local_tag_safe(self, tag, event, msg=None):
        self._modify_local_tags([(tag, event, msg)], with_future=False)

    def IsRemoteTagName(self, tagname):
        if not self.remote_tags_enabled:
            return False
        return ':' in tagname

    def AcquireTag(self, tagname):
        raw = self._RawTag(tagname)

        with self.lock:
            ret = self.inmem_items.setdefault(tagname, raw)

            if ret is raw and ret.IsCloud() and self._cloud: # no _cloud before Start()
                self._cloud.subscribe(tagname, with_future=False)

        return TagWrapper(ret)

    def _is_cloud_tag_name(self, name):
        if self.IsRemoteTagName(name):
            return False

        try:
            if self._tags_random_cloudiness:
                return hash(name) % 3 == 0

            if self._all_tags_in_cloud:
                return True

            return self._match_cloud_tag(name)

        except Exception as e:
            now = time.time()
            if now - self._last_tag_mask_error_report_time > 5:
                logging.error("Failed to match tag masks: %s" % e)
                self._last_tag_mask_error_report_time = now
            return False

    def _create_tag(self, name):
        if self.IsRemoteTagName(name):
            return RemoteTag(name, self._modify_local_tag_safe)
        elif self._is_cloud_tag_name(name):
            return CloudTag(name, self._modify_cloud_tag_safe)
        else:
            return LocalTag(name, self._modify_local_tag_safe)

    def _has_cloud_setup(self):
        return bool(self._cloud_tags_server)

    def vivify_tags_from_backup(self, tags):
        has_cloud_setup = self._has_cloud_setup()

        for tag in tags:
            if tag.IsCloud():
                if not has_cloud_setup:
                    raise RuntimeError("Cloud tags in backup, but no setup was found in config")
                modify = self._modify_cloud_tag_safe
            else:
                modify = self._modify_local_tag_safe

            tag._request_modify = modify

    def _make_tag_cloud(self, tag):
        assert isinstance(tag, LocalTag)

        tag.__class__ = CloudTag
        tag.version = 0
        self._set_modify_func(tag)

    def convert_in_memory_tags_to_cloud_if_need(self):
        if not self._has_cloud_setup():
            return False

        updates = []

        for tag_name, tag in self.inmem_items.iteritems():
            must_be_cloud = self._is_cloud_tag_name(tag_name) \
                and not tag.IsRemote() # Hack for disable_remote_tags

            if must_be_cloud == tag.IsCloud():
                continue

            elif must_be_cloud:
                if tag.IsLocallySet():
                    updates.append((tag_name, ETagEvent.Set))

                self._make_tag_cloud(tag)
            else:
                logging.error("Tag %s is cloud, but must not be" % tag_name)

        if not updates:
            return False

        logging.info("before conversion %d tags to CloudTag's" % len(updates))

        cloud = self._create_cloud_client(lambda ev: None)

        try:
            for bucket in split_in_groups(updates, 100000): # TODO Fix cloud_client.update
                cloud.update(bucket).get()
        finally:
            try:
                cloud.stop()
            except:
                logging.exception("Failed to stop temporary cloud client")

        logging.info("after conversion %d tags to CloudTag's" % len(updates))

        return True

    def make_on_disk_tags_conversion_params(self):
        if not self._has_cloud_setup():
            raise RuntimeError("No cloud tags setup")

        return OnDiskTagsConvertParams(
            db_filename=self.db_file,
            in_memory_tags=set(self.inmem_items.keys()),
            cloud_tags_server=self._cloud_tags_server
        )

    def _set_modify_func(self, tag):
        tag._request_modify = self._modify_cloud_tag_safe if tag.IsCloud() \
                              else self._modify_local_tag_safe

    def _GetTagLocalState(self, name):
        return self._RawTag(name, dont_create=True)

    def _RawTag(self, tagname, dont_create=False):
        if not tagname:
            raise ValueError("Empty tag name")

        tag = self.inmem_items.get(tagname, None)
        if tag:
            return tag

        if not self.db_file_opened:
            self.DBConnect()

        tagDescr = self.infile_items.get(tagname, None)

        if tagDescr:
            tag = cPickle.loads(tagDescr)

            if tag.IsCloud():
                if not self._has_cloud_setup():
                    logging.error("Tag %s is cloud on disk storage, but no setup for" \
                        " cloud in config. Restart server with proper setup!" % tagname)
            elif not tag.IsRemote(): # Hack for disable_remote_tags
                if self._is_cloud_tag_name(tag.GetFullname()):
                    logging.error("Tag %s is not cloud on disk storage, but must be." \
                        " Convert tags in disk storage!" % tagname)

            self._set_modify_func(tag)

        elif dont_create:
            return None

        else:
            tag = self._create_tag(tagname)

        return tag

    def _match_in_memory_tags(self, name_regex, prefix):
        for name, tag in self.inmem_items.items():
            if name and (not prefix or name.startswith(prefix)) \
                and (not name_regex or name_regex.match(name)):
                yield name, tag.IsLocallySet()

    def _match_in_file_tags(self, name_regex, prefix):
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

    def ListTags(self, regexp=None, prefix=None, memory_only=True):
        lists = []

        cloud_result = None
        if not memory_only and self._has_cloud_setup():
            cloud_result = self._cloud.match(
                prefix=prefix,
                regexp='^' + regexp.pattern if regexp else None
            )

        lists.append(list(self._match_in_memory_tags(regexp, prefix)))

        if not memory_only:
            lists.append(list(self._match_in_file_tags(regexp, prefix)))

        if cloud_result: # lock in future.get only after local db and inmemory
            lists[0:0] = [[(tag.tag_name, tag.is_set) for tag in cloud_result.get()]]

        if len(lists) == 1:
            return lists[0]

        ret = {}
        for lst in lists:
            for tag_name, is_set in lst:
                ret.setdefault(tag_name, is_set)

        return ret.items()

    def DBConnect(self):
        self.infile_items = bsddb3.btopen(self.db_file, "c")
        self.db_file_opened = True

    def UpdateContext(self, context):
        self.db_file = context.tags_db_file
        self.remote_tags_enabled = not context.disable_remote_tags
        self.DBConnect()
        self._journal.UpdateContext(context)
        self._repr_modifier.UpdateContext(context)
        self._cloud_tags_server = context.cloud_tags_server
        self._cloud_tags_masks = context.cloud_tags_masks
        self._cloud_tags_masks_reload_interval = context.cloud_tags_masks_reload_interval
        self._cloud_tags_release_delay = context.cloud_tags_release_delay
        self._tags_random_cloudiness = self._has_cloud_setup() and context.tags_random_cloudiness
        self._all_tags_in_cloud = self._has_cloud_setup() and context.all_tags_in_cloud

        logging.debug("TagStorage.UpdateContext, masks = %s, share = %s, server = %s" % (
            self._cloud_tags_masks, self._all_tags_in_cloud, self._cloud_tags_server))

    def Restore(self, timestamp):
        self._journal.Restore(timestamp, self, self._prev_safe_cloud_state)

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

        # TODO At this point GetListenersNumber and getrefcount may change

        with self.lock:
            if unsub_tags:
                self._cloud.unsubscribe(unsub_tags, with_future=False)

            for name in old_tags:
                tag = self.inmem_items.pop(name)
                tag.callbacks.clear()
                tag.__dict__.pop('_min_release_time', None) # FIXME

                try:
                    self.infile_items[name] = cPickle.dumps(tag, 2)
                except bsddb3.error as e:
                    if 'BSDDB object has already been closed' in e.message:
                        self.db_file_opened = False
                        self.db_file = None
                    raise

            self.infile_items.sync()


class MessageStorage(object):
    pass
