from __future__ import with_statement
import shutil
import time
import os
import sys
import re
from collections import deque
import gc
from common import PickableStdQueue, PickableStdPriorityQueue, as_rpc_user_error, RpcUserError, send_email, cleanup_directory
import common
from Queue import Empty, Queue as ThreadSafeQueue
import cStringIO
import StringIO
import itertools

import fork_locking
from job import FuncJob, FuncRunner
from common import Unpickable, PickableLock, PickableRLock, FakeObjectRegistrator, ObjectRegistrator, nullobject
from rem import PacketCustomLogic
from connmanager import ConnectionManager
from packet import LocalPacket, ReprState as PacketState
from queue import LocalQueue, SandboxQueue
from storages import PacketNamesStorage, TagStorage, ShortStorage, BinaryStorage, GlobalPacketStorage
from callbacks import ICallbackAcceptor, CallbackHolder, TagBase
import osspec
from rem.profile import ProfiledThread
from rem_logging import logger as logging


def _bind1st(f, arg):
    return lambda *args, **kwargs: f(arg, *args, **kwargs)


class SchedWatcher(Unpickable(tasks=PickableStdPriorityQueue.create,
                              lock=PickableLock,
                              workingQueue=PickableStdQueue.create
                            ),
                   ICallbackAcceptor,
                   CallbackHolder):
    def OnTick(self, ref):
        tm = time.time()
        while not self.tasks.empty():
            with self.lock:
                runtm, runner = self.tasks.peak()
                if runtm > tm:
                    break
                else:
                    self.tasks.get()
                    self.workingQueue.put(runner)
                    self.FireEvent("task_pending")

    def AddTaskD(self, deadline, fn, *args, **kws):
        if "skip_logging" in kws:
            skipLoggingFlag = bool(kws.pop("skip_logging"))
        else:
            skipLoggingFlag = False
        if not skipLoggingFlag:
            logging.debug("new task %r scheduled on %s", fn, time.ctime(deadline))
        self.tasks.put((deadline, (FuncRunner(fn, args, kws))))

    def AddTaskT(self, timeout, fn, *args, **kws):
        self.AddTaskD(time.time() + timeout, fn, *args, **kws)

    def GetTask(self):
        if not self.workingQueue.empty():
            try:
                return self.workingQueue.get_nowait()
            except Empty:
                logging.warning("Try to take task from emty SchedWatcher`s queue")
                return None
            except Exception:
                logging.exception("Some exception when SchedWatcher try take task")
                return None

    def has_startable_jobs(self):
        return not self.workingQueue.empty()

    def Empty(self):
        return not self.has_startable_jobs()

    def UpdateContext(self, context):
        self.AddNonpersistentCallbackListener(context.Scheduler)

    def ListTasks(self):
        with self.lock:
            return list(self.tasks.queue) \
                 + [(None, task) for task in list(self.workingQueue.queue)]

    def Clear(self):
        with self.lock:
            self.workingQueue.queue.clear()
            self.tasks.queue[:] = []

    def __len__(self):
        with self.lock:
            return len(self.workingQueue.queue) + len(self.tasks.queue)

    def __nonzero__(self):
        return bool(len(self))

    def __getstate__(self):
        # SchedWatcher can be unpickled for compatibility, but not pickled
        return {}

class QueueList(object):
    def __init__(self):
        self.__list = deque()
        self.__exists = set()

    def push(self, q):
        if q.name in self.__exists:
            raise KeyError("Stack already contains %s queue" % q.name)

        self.__exists.add(q.name)
        self.__list.append(q)

    def pop(self, *args):
        if args and not self.__list:
            return args[0] # default

        q = self.__list.popleft()
        self.__exists.remove(q.name)

        return q

    def __contains__(self, q):
        return q.name in self.__exists

    def __nonzero__(self):
        return bool(len(self.__exists))

    def __len__(self):
        return len(self.__exists)


class Mailer(object):
    _STOP_INDICATOR = object()

    def __init__(self, thread_count=1):
        self._queue = ThreadSafeQueue()

        self._threads = [
            ProfiledThread(target=self._worker, name_prefix='Mailer')
                for _ in xrange(thread_count)
        ]

        for t in self._threads:
            t.start()

    def stop(self):
        for _ in self._threads:
            self._queue.put(self._STOP_INDICATOR)

        for t in self._threads:
            t.join()

    def _worker(self):
        while True:
            task = self._queue.get()

            if task is self._STOP_INDICATOR:
                break

            try:
                send_email(*task)
            except Exception:
                logging.exception("Failed to send email {To: %s, Subject: %s}" % (task[0], task[1]))

    def send_async(self, rcpt, subj, body):
        #logging.debug("send_email_async(" + str(rcpt) + ")\n" + subj + "\n" + body)
        self._queue.put((rcpt, subj, body))


class Scheduler(Unpickable(lock=PickableRLock,
                           qRef=dict, #queues by name
                           tagRef=TagStorage, #inversed taglist for searhing tags by name
                           alive=(bool, False),
                           backupable=(bool, True),
                           queues_with_jobs=QueueList,
                           binStorage=BinaryStorage.create,
                           #storage with knowledge about saved binary objects (files for packets)
                           packStorage=GlobalPacketStorage, #storage of all known packets
                           tempStorage=ShortStorage,
                           #storage with knowledge about nonassigned packets (packets that was created but not yet assigned to appropriate queue)
                           schedWatcher=SchedWatcher, #watcher for time scheduled events
                           connManager=ConnectionManager, #connections to others rems
                           packetNamesTracker=PacketNamesStorage
                        ),
                ICallbackAcceptor):
    BackupFilenameMatchRe = re.compile("sched-(\d+).dump$")
    UnsuccessfulBackupFilenameMatchRe = re.compile("sched-\d*.dump.tmp$")
    SerializableFields = ["qRef", "tagRef", "binStorage", "tempStorage", "connManager"]
    BackupFormatVersion = 2

    def __init__(self, context):
        getattr(super(Scheduler, self), "__init__")()
        self.UpdateContext(context)
        self.tagRef.PreInit()
        self.ObjectRegistratorClass = ObjectRegistrator if context.register_objects_creation \
                                                      else FakeObjectRegistrator
        if context.use_memory_profiler:
            self.initProfiler()

    def UpdateContext(self, context=None):
        if context is not None:
            self.context = context
            self.poolSize = context.thread_pool_size
            self.initBackupSystem(context)
            context.registerScheduler(self)
        self.binStorage.UpdateContext(self.context)
        self.tagRef.UpdateContext(self.context)
        PacketCustomLogic.UpdateContext(self.context)
        self.connManager.UpdateContext(self.context)
        self.HasScheduledTask = fork_locking.Condition(self.lock)
        self.schedWatcher.UpdateContext(self.context)

    def initBackupSystem(self, context):
        self.backupPeriod = context.backup_period
        self.backupDirectory = context.backup_directory
        self.backupCount = context.backup_count
        self.backupInChild = context.backup_in_child

    def initProfiler(self):
        import guppy

        self.HpyInstance = guppy.hpy()
        self.LastHeap = None

    def rpc_delete_queue(self, qname):
        if qname in self.qRef:
            with self.lock:
                q = self.qRef.get(qname, None)
                if q:
                    if not q.Empty(): # race
                        raise RpcUserError(AttributeError("can't delete non-empty queue"))
                    self.qRef.pop(qname)
                    return True
        return False

    def _create_queue(self, name):
        cls = SandboxQueue if name.startswith('sbx:') else LocalQueue

        q = cls(name)
        self.qRef[name] = q

        q.UpdateContext(self.context)

        return q

    def _queue(self, name, create=True, from_rpc=False):
        with self.lock:
            q = self.qRef.get(name)
            if q:
                return q

            if not create:
                raise as_rpc_user_error(from_rpc, KeyError("Queue '%s' doesn't exist" % name))

            return self._create_queue(name)

    def rpc_get_queue(self, name, create=True):
        return self._queue(name, create, from_rpc=True)

    def _add_queue_as_non_empty_if_need(self, q):
        # this racy code may add empty queue to queues_with_jobs,
        # but it's better, than deadlock
        if q.has_startable_jobs():
            with self.lock:
                if q not in self.queues_with_jobs:
                    self.queues_with_jobs.push(q)
                    self.HasScheduledTask.notify()

    def get_job_to_run(self):
        with self.lock:
            while self.alive and not self.queues_with_jobs and self.schedWatcher.Empty():
                self.HasScheduledTask.wait()

            if not self.alive:
                return

            if not self.schedWatcher.Empty():
                func = self.schedWatcher.GetTask()
                if func:
                    return FuncJob(func)

            if self.queues_with_jobs:
                queue = self.queues_with_jobs.pop()

        if queue:
            # .get_job_to_run not under lock to prevent deadlock with Notify
            job = queue.get_job_to_run(self.context)
            #if job:
                #logging.debug('ThreadJobWorker get_job_to_run %s from %s' % (job, job.pck))

            self._add_queue_as_non_empty_if_need(queue)

            return job # may be None

        logging.warning("No tasks for execution after condition waking up")

    def Notify(self, ref):
        if isinstance(ref, LocalQueue):
            self._add_queue_as_non_empty_if_need(ref)

        elif isinstance(ref, SchedWatcher):
            if ref.Empty():
                return
            with self.lock:
                if not ref.Empty():
                    self.HasScheduledTask.notify()


    def CheckBackupFilename(self, filename):
        return bool(self.BackupFilenameMatchRe.match(filename))

    def ExtractTimestampFromBackupFilename(self, filename):
        name = os.path.basename(filename)
        if self.CheckBackupFilename(name):
            return int(self.BackupFilenameMatchRe.match(name).group(1))
        return None

    def CheckUnsuccessfulBackupFilename(self, filename):
        return bool(self.UnsuccessfulBackupFilenameMatchRe.match(filename))

    @common.logged()
    def forgetOldItems(self):
        for queue_name, queue in self.qRef.copy().iteritems():
            queue.forgetOldItems()

        self.binStorage.forgetOldItems()
        self.tempStorage.forgetOldItems()
        self.tagRef.tofileOldItems()

    @common.logged()
    def RollBackup(self, force=False, child_max_working_time=None):
        child_max_working_time = child_max_working_time \
            or self.context.backup_child_max_working_time

        if not os.path.isdir(self.backupDirectory):
            os.makedirs(self.backupDirectory)

        self.forgetOldItems()
        gc.collect() # for Packet -> Job -> Packet cyclic references

        start_time = time.time()

        self.tagRef._journal.Rotate(start_time)

        if not self.backupable and not force:
            logging.warning("REM is currently not in backupable state; change it back to backupable as soon as possible")
            return

        def backup(fast_strings):
            self.SaveBackup(
                os.path.join(self.backupDirectory, "sched-%.0f.dump" % start_time),
                cStringIO.StringIO if fast_strings else StringIO.StringIO
            )

        if self.backupInChild:
            child = fork_locking.run_in_child(lambda : backup(True), child_max_working_time)

            logging.debug("backup fork stats: %s", {k: round(v, 3) for k, v in child.timings.items()})

            if child.errors:
                logging.warning("Backup child process stderr: " + child.errors)

            if child.term_status:
                raise RuntimeError("Child process failed to write backup: %s" \
                    % osspec.repr_term_status(child.term_status))
        else:
            backup(False)

        backupFiles = sorted(filter(self.CheckBackupFilename, os.listdir(self.backupDirectory)), reverse=True)
        unsuccessfulBackupFiles = filter(self.CheckUnsuccessfulBackupFilename, os.listdir(self.backupDirectory))
        for filename in backupFiles[self.backupCount:] + unsuccessfulBackupFiles:
            os.unlink(os.path.join(self.backupDirectory, filename))

        self.tagRef._journal.Clear(start_time - self.context.journal_lifetime)

    def SuspendBackups(self):
        self.backupable = False

    def ResumeBackups(self):
        self.backupable = True

    def DisableBackupsInChild(self):
        self.backupInChild = False

    def EnableBackupsInChild(self):
        self.backupInChild = True

    def Serialize(self, out):
        import cPickle as pickle

        sdict = {k: getattr(self, k) for k in self.SerializableFields}
        sdict['qRef'] = sdict['qRef'].copy()
        sdict['format_version'] = self.BackupFormatVersion

        p = pickle.Pickler(out, 2)
        p.dump(sdict)

    def SaveBackup(self, filename, string_cls=StringIO.StringIO):
        tmpFilename = filename + ".tmp"
        with open(tmpFilename, "w") as out:
            mem_out = string_cls()
            try:
                self.Serialize(mem_out)
                out.write(mem_out.getvalue())
            finally:
                mem_out.close()

        os.rename(tmpFilename, filename)

        if self.context.use_memory_profiler:
            try:
                last_heap = self.LastHeap
                self.LastHeap = self.HpyInstance.heap()
                heapsDiff = self.LastHeap.diff(last_heap) if last_heap else self.LastHeap
                logging.info("memory changes: %s", heapsDiff)
                logging.debug("GC collecting result %s", gc.collect())
            except Exception, e:
                logging.exception("%s", e)

    def __reduce__(self):
        return nullobject, ()

    @classmethod
    def Deserialize(cls, stream, additional_objects_registrator=FakeObjectRegistrator()):
        import packet
        import cPickle as pickle

        class Registrator(object):
            def __init__(self):
                self.packets = deque()
                self.tags = deque()

            def register(self, obj, state):
                if isinstance(obj, packet.PacketBase):
                    self.packets.append(obj)
                elif isinstance(obj, TagBase):
                    self.tags.append(obj)

            def LogStats(self):
                pass

        registrator = Registrator()

        common.ObjectRegistrator_ = objects_registrator \
            = common.ObjectRegistratorsChain([
                registrator,
                additional_objects_registrator
            ])

        unpickler = pickle.Unpickler(stream)

        try:
            sdict = unpickler.load()
            assert isinstance(sdict, dict)
        finally:
            common.ObjectRegistrator_ = FakeObjectRegistrator()

        format_version = sdict.pop('format_version', 1)

        sdict = {k: sdict[k] for k in cls.SerializableFields + ['schedWatcher'] if k in sdict}

        objects_registrator.LogStats()

        # TODO ATW each packet exists in register in 2 copies
        registrator.packets = list(set(registrator.packets))

        cls._convert_backup(format_version, sdict, registrator)

        return sdict, registrator

    @classmethod
    def DeserializeFile(cls, filename, registrator=FakeObjectRegistrator()):
        with open(filename, 'r') as stream:
            return cls.Deserialize(stream, registrator)

    def LoadBackup(self, filename, restorer=None, restore_tags_only=False):
        with self.lock:
            with open(filename, "r") as stream:
                sdict, registrator = self.Deserialize(stream, self.ObjectRegistratorClass())

            if restorer:
                restorer(sdict, registrator)

            qRef = sdict.pop("qRef")
            prevWatcher = sdict.pop("schedWatcher", None) # from old backups

            self.__setstate__(sdict)

            self.UpdateContext(None)

            self.tagRef.PreInit()

            tagStorage = self.tagRef

            tagStorage.vivify_tags_from_backup(registrator.tags)

            for pck in registrator.packets:
                pck.vivify_done_tags_if_need(tagStorage)

            self.tagRef.Restore(self.ExtractTimestampFromBackupFilename(filename) or 0)

            if restore_tags_only:
                self.qRef = qRef
                return

            self._vivify_queues(qRef)

            # No vivifying of tempStorage packets

            self.schedWatcher.Clear() # remove tasks from Queue.relocate_packet
            #self.FillSchedWatcher(prevWatcher)

    @classmethod
    def _convert_backup(cls, sdict_version, sdict, registrator):
        for from_version in range(sdict_version, cls.BackupFormatVersion):
            getattr(cls, '_convert_backup_to_v%d' % (from_version + 1))(sdict, registrator)

    @classmethod
    def _convert_backup_to_v2(cls, sdict, registrator):
        for pck in registrator.packets:
            pck.convert_to_v2()
            pck.__class__ = LocalPacket

    @classmethod
    def _make_on_disk_tags_conversion_params(cls, ctx):
        import cPickle as pickle

        pipe_rd, pipe_wr = os.pipe()

        pid = os.fork()

        if not pid:
            try:
                os.close(pipe_rd)

                sched = cls(ctx)
                sched.Restore(restore_tags_only=True)

                params = sched.tagRef.make_on_disk_tags_conversion_params()

                with os.fdopen(pipe_wr, 'w') as out:
                    pickle.dump(params, out, pickle.HIGHEST_PROTOCOL)

            except:
                try:
                    logging.exception("Failed to collect and dump in-memory tags")
                except:
                    pass
                os._exit(1)
            else:
                os._exit(0)

        os.close(pipe_wr)

        with os.fdopen(pipe_rd, 'r') as in_:
            params = pickle.load(in_)

        _, status = os.waitpid(pid, 0)

        if status:
            raise RuntimeError("Child process failed to produce OnDiskTagsConvertParams: %s" \
                % osspec.repr_term_status(status))

        return params

    @classmethod
    def convert_on_disk_tags_to_cloud(cls, ctx, yt_writer_count=20, bucket_size=2500):
        import tags_conversion

        params = cls._make_on_disk_tags_conversion_params(ctx)

        logging.debug("convert_params=%r" % params)

        tags_conversion.convert_on_disk_tags_to_cloud(
            db_filename=params.db_filename,
            in_memory_tags=params.in_memory_tags,
            cloud_tags_server_addr=params.cloud_tags_server,
            yt_writer_count=yt_writer_count,
            bucket_size=bucket_size
        )

    def FillSchedWatcher(self, prev_watcher=None):
        assert False

        def list_packets_in_queues(state):
            return [
                pck for q in self.qRef.itervalues()
                    for pck in q.ListAllPackets()
                        if pck.state == state
            ]

        def list_schedwatcher_tasks(obj_type, method_name):
            if not prev_watcher:
                return []

            return (
                (deadline, task)
                    for deadline, task in prev_watcher.ListTasks()
                        if task.object \
                            and isinstance(task.object, obj_type) \
                            and task.methName == method_name
            )

        def produce_packets_to_wait():
            packets = list_packets_in_queues(PacketState.WAITING)

            logging.debug("WAITING packets in Queue's for schedWatcher: %s" % [pck.id for pck in packets])

            now = time.time()

            prev_deadlines = {
                task.object.id: deadline or now
                    for deadline, task in list_schedwatcher_tasks(LocalPacket, 'stopWaiting')
            }

            if prev_watcher:
                logging.debug("old backup schedWatcher WAITING packets deadlines: %s" % prev_deadlines)

            for pck in packets:
                pck.waitingDeadline = pck.waitingDeadline \
                    or prev_deadlines.get(pck.id, None) \
                    or time.time() # missed in old backup

            return packets

        for pck in produce_packets_to_wait():
            self.ScheduleTaskD(pck.waitingDeadline, pck.stopWaiting)

    def Restore(self, restorer=None, restore_tags_only=False):
        ctx = self.context

        if not os.path.isdir(ctx.backup_directory):
            return

        backup_filename = None

        for name in sorted(os.listdir(ctx.backup_directory), reverse=True):
            if self.CheckBackupFilename(name):
                backup_filename = os.path.join(ctx.backup_directory, name)
                break

        if not backup_filename:
            self.tagRef.Restore(0)
            return

        if restorer:
            restorer = _bind1st(restorer, self)

        try:
            self.LoadBackup(backup_filename, restorer=restorer, restore_tags_only=restore_tags_only)
        except Exception:
            t, v, tb = sys.exc_info()
            logging.exception("can't restore from '%s'", backup_filename)
            raise t, v, tb

        if restore_tags_only:
            return

        if ctx.allow_startup_tags_conversion:
            self.tagRef.convert_in_memory_tags_to_cloud_if_need()

    def cleanup_bin_storage_fs(self):
        self.binStorage.cleanup_fs()

    def cleanup_packet_storage_fs(self):
        to_keep = set(self.packStorage.ids())
        to_keep |= set(self.tempStorage.ids())
        cleanup_directory(self.context.packets_directory, to_keep)

    def _vivify_queues(self, qRef):
        for name, q in qRef.iteritems():
            self._vivify_queue(q)
            self.qRef[name] = q

    def _vivify_queue(self, q):
        ctx = self.context

        q.UpdateContext(ctx)
        q.scheduler = self

        for pck in list(q.ListAllPackets()):
            pck.update_tag_deps(self.tagRef)
            pck.try_recover_after_backup_loading(ctx)
            q.relocate_packet(pck) # j.i.c force?

            self.packStorage.Add(pck)

            if pck.state != PacketState.HISTORIED:
                self.packetNamesTracker.Add(pck.name)
                pck.AddCallbackListener(self.packetNamesTracker)

        self._add_queue_as_non_empty_if_need(q)

    def AddPacketToQueue(self, qname, pck):
        queue = self._queue(qname)
        self.packStorage.Add(pck)
        pck._attach_to_queue(queue)
        self.packetNamesTracker.Add(pck.name)
        pck.AddCallbackListener(self.packetNamesTracker)

    def RegisterNewPacket(self, pck, wait_tags):
        for tag in wait_tags:
            self.connManager.Subscribe(tag)
        self.tempStorage.StorePacket(pck)

    def GetPacket(self, pck_id):
        return self.packStorage.GetPacket(pck_id)

    def ScheduleTaskD(self, deadline, fn, *args, **kws):
        self.schedWatcher.AddTaskD(deadline, fn, *args, **kws)

    def ScheduleTaskT(self, timeout, fn, *args, **kws):
        self.schedWatcher.AddTaskT(timeout, fn, *args, **kws)

    def Start(self):
        for q in self.qRef.itervalues():
            for pck in list(q.ListAllPackets()):
                pck.vivify_jobs_waiting_stoppers()

        self._mailer = Mailer(self.context.mailer_thread_count)
        logging.debug("after_mailer_start")

        self.tagRef.Start()
        logging.debug("after_tag_storage_start")

        with self.lock:
            self.alive = True
            self.HasScheduledTask.notify_all()

        self.connManager.Start()
        logging.debug("after_connection_manager_start")

    def Stop1(self):
        self.connManager.Stop()
        with self.lock:
            self.alive = False
            self.HasScheduledTask.notify_all()

    def Stop2(self):
        self.tagRef.Stop()

    def Stop3(self):
        self._mailer.stop()
        PacketCustomLogic.UpdateContext(None)

    def Stop(self):
        self.Stop1()
        self.Stop2()
        self.Stop3()

    def GetConnectionManager(self):
        return self.connManager

    def OnTaskPending(self, ref):
        self.Notify(ref)

    def _on_job_pending(self, queue):
        self.Notify(queue)

    def send_email_async(self, rcpt, (subj, body)):
        self._mailer.send_async(rcpt, subj, body)
