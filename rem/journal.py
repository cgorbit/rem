from __future__ import with_statement
import bsddb3
import cPickle
import logging
import os

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


class TagLogger(Unpickable(lock=PickableRLock), ICallbackAcceptor):
    def __init__(self, tagRef):
        super(TagLogger, self).__init__()
        self.file = None
        self.tagRef = tagRef
        self.restoring_mode = False
        self.file_opened = False

    def Open(self, filename):
        self.file = bsddb3.rnopen(filename, "c")
        self.file_opened = True

    def UpdateContext(self, context):
        self.db_file = context.recent_tags_file
        self.Open(self.db_file)

    def LockedAppend(self, data):
        if not self.restoring_mode:
            if not self.file_opened:
                with self.lock:
                    self.Open(self.db_file)
            with self.lock:
                try:
                    key = self.file.last()[0] + 1
                except bsddb3.error as e:
                    if 'BSDDB object has already been closed' in e.message:
                        self.file_opened = False
                        self.file = None
                        raise
                    else:
                        key = 1
                self.file[key] = data
            self.file.sync()

    def LogEvent(self, cls, *args, **kws):
        obj = cls(*args, **kws)
        self.LockedAppend(cPickle.dumps(obj))

    def OnDone(self, tag):
        if isinstance(tag, (Tag, RemoteTag)):
            self.LogEvent(SetTagEvent, tag.GetFullname())

    def OnUndone(self, tag):
        self.LogEvent(UnsetTagEvent, tag.GetFullname())

    def OnReset(self, (tag, message)):
        self.LogEvent(ResetTagEvent, tag.GetFullname(), message)

    def Restore(self, timestamp):
        logging.debug("TagLogger.Restore(%d)", timestamp)
        dirname, db_filename = os.path.split(self.db_file)

        def get_filenames():
            result = []
            for filename in os.listdir(dirname):
                if filename.startswith(db_filename) and filename != db_filename:
                    file_time = int(filename.split("-")[-1])
                    if file_time > timestamp:
                        result.append(filename)
            result = sorted(result)
            if os.path.isfile(self.db_file):
                result += [db_filename]
            return result

        with self.lock:
            self.restoring_mode = True
            for filename in get_filenames():
                f = bsddb3.rnopen(os.path.join(dirname, filename), "r")
                for k, v in f.items():
                    try:
                        obj = cPickle.loads(v)
                        obj.Redo(self)
                    except Exception, e:
                        logging.exception("occurred in TagLogger while restoring from a journal : %s", e)
                f.close()
            self.restoring_mode = False

    def Rotate(self, timestamp):
        logging.info("TagLogger.Rotate")
        with self.lock:
            self.file.close()
            if os.path.exists(self.db_file):
                new_filename = "%s-%d" % (self.db_file, timestamp)
                os.rename(self.db_file, new_filename)
            self.Open(self.db_file)

    def Clear(self, final_time):
        logging.info("TagLogger.Clear(%s)", final_time)
        dirname, db_filename = os.path.split(self.db_file)
        for filename in os.listdir(dirname):
            if filename.startswith(db_filename) and filename != db_filename:
                file_time = int(filename.split("-")[-1])
                if file_time <= final_time:
                    os.remove(os.path.join(dirname, filename))
