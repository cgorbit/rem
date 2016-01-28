from __future__ import with_statement
import bsddb3
import cPickle
import logging
import time
import os
import threading
from collections import deque

from common import Unpickable, PickableRLock
from callbacks import ICallbackAcceptor, RemoteTag, Tag


class TagEvent(object):
    def __init__(self, tagname):
        self.tagname = tagname

    def Redo(self, journal):
        self.DoRedo(journal.tagRef.AcquireTag(self.tagname))


class SetTagEvent(TagEvent):
    def DoRedo(self, tag):
        tag._Set()


class UnsetTagEvent(TagEvent):
    def DoRedo(self, tag):
        tag._Unset()


class ResetTagEvent(TagEvent, Unpickable(message=str)):
    def __init__(self, tagname, message):
        super(ResetTagEvent, self).__init__(tagname)
        self.message = message

    def DoRedo(self, tag):
        tag._Reset(self.message)


class JournalDB(object):
    def __init__(self, filename):
        self._impl = bsddb3.rnopen(filename, "c")

    def _get_next_key(self):
        try:
            return self._impl.last()[0] + 1
        except bsddb3._pybsddb.DBNotFoundError:
            return 1

    def write(self, data):
        self._impl[self._get_next_key()] = data

    def close(self):
        self._impl.close()

    def sync(self):
        self._impl.sync()


class TagLogger(Unpickable(lock=PickableRLock), ICallbackAcceptor):
    def __init__(self, tagRef):
        super(TagLogger, self).__init__()
        self._db = None
        self.db_filename = None
# TODO For restore pass tagRef explicitely
        self.tagRef = tagRef
        self._restoring_mode = False
        self._should_stop = False
        self._queue = deque()
        self._queue_lock = threading.Lock()
        self._db_lock = threading.Lock()
        self._queue_not_empty = threading.Condition(self._queue_lock)
        self._write_thread = threading.Thread(target=self._write_loop)
        self._write_thread.start()

    def Stop(self):
        with self._queue_lock:
            self._should_stop = True
            self._queue_not_empty.notify()
        self._write_thread.join()

    def _reopen(self):
        if self.db_filename is None:
            raise RuntimeError("db_filename is not yet set")
        self._db = JournalDB(self.db_filename)

    def UpdateContext(self, context):
        with self._db_lock:
            self.db_filename = context.recent_tags_file
            self._reopen()

    def _write(self, data):
        timeout = 1.0
        max_timeout = 15.0

        while True:
            with self._db_lock:
                try:
                    if not self._db:
                        self._reopen()
                    self._db.write(data)
                    self._db.sync()
                except Exception as err:
                    self._db = None
                    logging.error("Can't write to journal (%d items left): %s" \
                        % (len(self._queue), err))
                else:
                    break

            timeout = min(max_timeout, timeout * 2)
            time.sleep(timeout)

    def _write_loop(self):
        while True:
            with self._queue_lock:
                while not(self._queue or self._should_stop):
                    self._queue_not_empty.wait()

                if self._should_stop and not self._queue:
                    return

            while self._queue:
                self._write(cPickle.dumps(self._queue.pop()))

    def _LogEvent(self, cls, *args, **kws):
        obj = cls(*args, **kws)
        with self._queue_lock:
            self._queue.append(obj)
            self._queue_not_empty.notify()

    def OnDone(self, tag):
        if isinstance(tag, (Tag, RemoteTag)):
            self._LogEvent(SetTagEvent, tag.GetFullname())

    def OnUndone(self, tag):
        self._LogEvent(UnsetTagEvent, tag.GetFullname())

    def OnReset(self, (tag, message)):
        self._LogEvent(ResetTagEvent, tag.GetFullname(), message)

    def Restore(self, timestamp):
        logging.debug("TagLogger.Restore(%d)", timestamp)
        dirname, db_filename = os.path.split(self.db_filename)

        def get_filenames():
            result = []
            for filename in os.listdir(dirname):
                if filename.startswith(db_filename) and filename != db_filename:
                    file_time = int(filename.split("-")[-1])
                    if file_time > timestamp:
                        result.append(filename)
            result = sorted(result)
            if os.path.isfile(self.db_filename):
                result += [db_filename]
            return result

        with self._db_lock:
            self._restoring_mode = True
            for filename in get_filenames():
                f = bsddb3.rnopen(os.path.join(dirname, filename), "r")
                for k, v in f.items():
                    try:
                        obj = cPickle.loads(v)
                        obj.Redo(self)
                    except Exception, e:
                        logging.exception("occurred in TagLogger while restoring from a journal : %s", e)
                f.close()
            self._restoring_mode = False

    def Rotate(self, timestamp):
        logging.info("TagLogger.Rotate")

        with self._db_lock:
            if self._db:
                self._db.sync()
                self._db.close()

            if os.path.exists(self.db_filename):
                new_filename = "%s-%d" % (self.db_filename, timestamp)
                os.rename(self.db_filename, new_filename)

            self._reopen()

    def Clear(self, final_time):
        logging.info("TagLogger.Clear(%s)", final_time)
        dirname, db_filename = os.path.split(self.db_filename)
        for filename in os.listdir(dirname):
            if filename.startswith(db_filename) and filename != db_filename:
                file_time = int(filename.split("-")[-1])
                if file_time <= final_time:
                    os.remove(os.path.join(dirname, filename))
