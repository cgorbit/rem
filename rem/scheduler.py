from __future__ import with_statement
import shutil
import time
import logging
import os
import re
from collections import deque
import gc
from common import PickableStdQueue, PickableStdPriorityQueue
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
from packet import JobPacket, PacketState, PacketFlag
from queue import Queue
from storages import PacketNamesStorage, TagStorage, ShortStorage, BinaryStorage, GlobalPacketStorage
from callbacks import ICallbackAcceptor, CallbackHolder, TagBase
import osspec
from rem.profile import ProfiledThread

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

    def HasStartableJobs(self):
        return not self.workingQueue.empty()

    def Empty(self):
        return not self.HasStartableJobs()

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
    __slots__ = ['__list', '__exists']

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

            osspec.send_email(*task)

    def send_async(self, rcpt, subj, body):
        logging.debug("send_email_async(" + str(rcpt) + ")\n" + subj + "\n" + body)
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

    def __init__(self, context):
        getattr(super(Scheduler, self), "__init__")()
        self.UpdateContext(context)
        self.tagRef.PreInit()
        self.ObjectRegistratorClass = FakeObjectRegistrator if context.execMode == "start" else ObjectRegistrator
        if context.useMemProfiler:
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

    def OnWaitingStart(self, ref):
        if isinstance(ref, JobPacket):
            self.ScheduleTaskD(ref.waitingDeadline, ref.stopWaiting)

    def initBackupSystem(self, context):
        self.backupPeriod = context.backup_period
        self.backupDirectory = context.backup_directory
        self.backupCount = context.backup_count
        self.backupInChild = context.backup_in_child

    def initProfiler(self):
        import guppy

        self.HpyInstance = guppy.hpy()
        self.LastHeap = None

    def DeleteUnusedQueue(self, qname):
        if qname in self.qRef:
            with self.lock:
                q = self.qRef.get(qname, None)
                if q:
                    if not q.Empty():
                        raise AttributeError("can't delete non-empty queue")
                    self.qRef.pop(qname)
                    return True
        return False

    def _create_queue(self, name):
        q = Queue(name)
        self.qRef[name] = q

        q.UpdateContext(self.context)
        q.AddCallbackListener(self)

        return q

    def Queue(self, name, create=True):
        with self.lock:
            q = self.qRef.get(name)
            if q:
                return q

            if not create:
                raise KeyError("Queue '%s' doesn't exist" % name)

            return self._create_queue(name)

    def _add_queue_as_non_empty_if_need(self, q):
        # this racy code may add empty queue to queues_with_jobs,
        # but it's better, than deadlock
        if q.HasStartableJobs():
            with self.lock:
                if q not in self.queues_with_jobs:
                    self.queues_with_jobs.push(q)
                    self.HasScheduledTask.notify()

    def Get(self):
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
            # .Get not under lock to prevent deadlock with Notify
            job = queue.Get(self.context)
            if job:
                logging.debug('ThreadJobWorker get_job_to_run %s from %s' % (job, job.pck))

            self._add_queue_as_non_empty_if_need(queue)

            return job # may be None

        logging.warning("No tasks for execution after condition waking up")

    def Notify(self, ref):
        if isinstance(ref, Queue):
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
        gc.collect() # for JobPacket -> Job -> JobPacket cyclic references

        start_time = time.time()

        self.tagRef.tag_logger.Rotate(start_time)

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

            logging.debug("backup fork stats: %s", child.timings)

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

        self.tagRef.tag_logger.Clear(start_time - self.context.journal_lifetime)

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

        if self.context.useMemProfiler:
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

        class PacketsRegistrator(object):
            def __init__(self):
                self.packets = deque()
                self.tags = deque()

            def register(self, obj, state):
                if isinstance(obj, packet.JobPacket):
                    self.packets.append(obj)
                elif isinstance(obj, TagBase):
                    self.tags.append(obj)

            def LogStats(self):
                pass

        packets_registrator = PacketsRegistrator()

        common.ObjectRegistrator_ = objects_registrator \
            = common.ObjectRegistratorsChain([
                packets_registrator,
                additional_objects_registrator
            ])

        unpickler = pickle.Unpickler(stream)

        try:
            sdict = unpickler.load()
            assert isinstance(sdict, dict)
        finally:
            common.ObjectRegistrator_ = FakeObjectRegistrator()

        sdict = {k: sdict[k] for k in cls.SerializableFields + ['schedWatcher'] if k in sdict}

        objects_registrator.LogStats()

        return sdict, packets_registrator

    def LoadBackup(self, filename, restorer=None):
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

            #for tag in registrator.tags:
                # FIXME nothing listeners to drop
            tagStorage.vivify_tags(registrator.tags)

            for pck in registrator.packets:
                pck.VivifyDoneTagsIfNeed(tagStorage)

            self.tagRef.Restore(self.ExtractTimestampFromBackupFilename(filename) or 0)

            self._vivify_queues(qRef)

            self.schedWatcher.Clear() # remove tasks from Queue.relocatePacket
            self.FillSchedWatcher(prevWatcher)

    def FillSchedWatcher(self, prev_watcher=None):
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
                    for deadline, task in list_schedwatcher_tasks(JobPacket, 'stopWaiting')
            }

            if prev_watcher:
                logging.debug("old backup schedWatcher WAITING packets deadlines: %s" % prev_deadlines)

            for pck in packets:
                pck.waitingDeadline = pck.waitingDeadline \
                    or prev_deadlines.get(pck.id, None) \
                    or time.time() # missed in old backup

            return packets

        def list_noninitialized_packets():
            packets1 = list_packets_in_queues(PacketState.NONINITIALIZED)

            logging.debug("NONINITIALIZED packets in Queue's for schedWatcher: %s" % [pck.id for pck in packets1])

            packets2 = [
                task.args[0]
                    for _, task in list_schedwatcher_tasks(Queue, 'RestoreNoninitialized')]

            if prev_watcher:
                logging.debug("NONINITIALIZED packets in old schedWatcher: %s" % [pck.id for pck in packets2])

            return packets1 + packets2

        for pck in produce_packets_to_wait():
            self.ScheduleTaskD(pck.waitingDeadline, pck.stopWaiting)

        # XXX
        # from :rem-20-more-packet-locks-2 fork_locking guarantee that
        # backups will not contain packets in NONINITIALIZED,
        # _but_ not all servers ATW has enabled backup_in_child option

        ctx = self.context
        for pck in list_noninitialized_packets():
            pck.try_recover_noninitialized_from_backup(ctx)

    def _vivify_queues(self, qRef):
        for name, q in qRef.iteritems():
            self._vivify_queue(q)
            self.qRef[name] = q

    def _vivify_queue(self, q):
        ctx = self.context

        q.UpdateContext(ctx)
        q.AddCallbackListener(self)

        for pck in list(q.ListAllPackets()):
            pck.UpdateTagDependencies(self.tagRef)
            pck.try_recover_after_backup_loading(ctx)
            q.relocatePacket(pck) # j.i.c force?

            self.packStorage.Add(pck)

            if pck.state != PacketState.HISTORIED:
                self.packetNamesTracker.Add(pck.name)
                pck.AddCallbackListener(self.packetNamesTracker)

        self._add_queue_as_non_empty_if_need(q)

    def AddPacketToQueue(self, qname, pck):
        queue = self.Queue(qname)
        self.packStorage.Add(pck)
        pck._move_to_queue(None, queue)
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
        self._mailer = Mailer(self.context.mailer_thread_count)
        self.tagRef.Start()
        with self.lock:
            self.alive = True
            self.HasScheduledTask.notify_all()
        self.connManager.Start()

    def Stop1(self):
        self.connManager.Stop()
        with self.lock:
            self.alive = False
            self.HasScheduledTask.notify_all()

    def Stop2(self):
        self.tagRef.Stop()

    def Stop3(self):
        self._mailer.stop()

    def GetConnectionManager(self):
        return self.connManager

    def OnTaskPending(self, ref):
        self.Notify(ref)

    #def OnPacketReinitRequest(self, code):
        #code(self.context)

    def send_email_async(self, rcpt, (subj, body)):
        self._mailer.send_async(rcpt, subj, body)
