from __future__ import with_statement
import logging
import tempfile
import os
import time
import shutil
import errno

from callbacks import CallbackHolder, ICallbackAcceptor, TagBase, tagset
from common import BinaryFile, PickableRLock, Unpickable, safeStringEncode, get_None
from job import Job, PackedExecuteResult
import osspec
import fork_locking
import messages

class PacketState(object):
    CREATED = "CREATED"
    WORKABLE = "WORKABLE"               # working packet without pending jobs
    PENDING = "PENDING"                 # working packet with pending jobs (may have running jobs)
    SUSPENDED = "SUSPENDED"             # waiting for tags or manually suspended (may have running jobs)
    ERROR = "ERROR"                     # unresolved error exists
    SUCCESSFULL = "SUCCESSFULL"         # successfully done packet
    HISTORIED = "HISTORIED"             # temporary state before removal
    WAITING = "WAITING"                 # wait timeout for retry failed jobs
    NONINITIALIZED = "NONINITIALIZED"
    # src -> dst
    allowed = {
        CREATED: (WORKABLE, SUSPENDED, NONINITIALIZED),
        WORKABLE: (SUSPENDED, ERROR, SUCCESSFULL, PENDING, WAITING, NONINITIALIZED),
        PENDING: (SUSPENDED, WORKABLE, ERROR, WAITING, NONINITIALIZED),
        SUSPENDED: (WORKABLE, HISTORIED, WAITING, ERROR, NONINITIALIZED),
        WAITING: (PENDING, SUSPENDED, ERROR, NONINITIALIZED),
        ERROR: (SUSPENDED, HISTORIED, NONINITIALIZED),
        SUCCESSFULL: (HISTORIED, NONINITIALIZED),
        NONINITIALIZED: (CREATED,),
        HISTORIED: ()}


class PacketFlag:
    USER_SUSPEND = 0x01              #suspended by user
    RCVR_ERROR = 0x02              #recovering error detected

class PacketCustomLogic(object):
    SchedCtx = None

    @classmethod
    def UpdateContext(cls, context):
        cls.SchedCtx = context


class JobPacketImpl(object):
    """tags manipulation methods"""

    def _SetWaitingTags(self, wait_tags):
        for tag in wait_tags:
            tag.AddCallbackListener(self)
        self.allTags = set(tag.GetFullname() for tag in wait_tags)
        self.waitTags = set(tag.GetFullname() for tag in wait_tags if not tag.IsLocallySet())

    def _ProcessTagSetEvent(self, tag):
        with self.lock:
            tagname = tag.GetFullname()
            if tagname in self.waitTags:
                self.waitTags.remove(tagname)
                if not self.waitTags and self.state == PacketState.SUSPENDED:
                    self.Resume()

    def VivifyDoneTagsIfNeed(self, tagStorage):
        with self.lock:
            if isinstance(self.done_indicator, str):
                self.done_indicator = tagStorage.AcquireTag(self.done_indicator)
            for jid, cur_val in self.job_done_indicator.iteritems():
                if isinstance(cur_val, str):
                    self.job_done_indicator[jid] = tagStorage.AcquireTag(cur_val)

    def UpdateTagDependencies(self, tagStorage):
        with self.lock:
            if isinstance(self.done_indicator, TagBase):
                self.done_indicator = tagStorage.AcquireTag(self.done_indicator.name)
            for jid in self.job_done_indicator:
                if isinstance(self.job_done_indicator[jid], TagBase):
                    self.job_done_indicator[jid] = tagStorage.AcquireTag(self.job_done_indicator[jid].name)

            self.waitTags = tagset(self.waitTags)
            for tag in map(tagStorage.AcquireTag, self.waitTags):
                if tag.IsLocallySet():
                    self._ProcessTagSetEvent(tag)

    def _ReleaseLinks(self):
        tmpLinks, self.binLinks = self.binLinks, {}
        while tmpLinks:
            binname, file = tmpLinks.popitem()
            if isinstance(file, BinaryFile):
                # until race condition in changeState will be fixed
                try:
                    file.Unlink(self, binname)
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        logging.exception("Packet %s release place error", self.id)
                    else:
                        raise
                filehash = file.checksum
            elif isinstance(file, str):
                filehash = file
            else:
                filehash = None
            if filehash is not None:
                self.binLinks[binname] = filehash

    def _CreateLink(self, binname, file):
        file.Link(self, binname)
        self.binLinks[binname] = file

    def _AddLink(self, binname, file):
        if binname in self.binLinks:
            old_file = self.binLinks.pop(binname)
            if isinstance(old_file, BinaryFile):
                old_file.Unlink(self, binname)
        self._CreateLink(binname, file)

    def _VivifyLink(self, context, link):
        if isinstance(link, str):
            link = context.Scheduler.binStorage.GetFileByHash(link)
        elif isinstance(link, BinaryFile):
            link = context.Scheduler.binStorage.GetFileByHash(link.checksum)
        return link

    def _CreateLinks(self, context):
        tmpLinks, self.binLinks = self.binLinks, {}
        while tmpLinks:
            binname, link = tmpLinks.popitem()
            file = self._VivifyLink(context, link)
            if file is not None:
                self._CreateLink(binname, file)

    def AreLinksAlive(self, context):
        with self.lock:
            return all(self._VivifyLink(context, link) for link in self.binLinks.itervalues())

# FIXME race from storages
    def ReleasePlace(self):
        with self.lock:
            self._ReleaseLinks()
            if self.directory and os.path.isdir(self.directory):
                try:
                    shutil.rmtree(self.directory, onerror=None)
                except Exception, e:
                    logging.exception("Packet %s release place error", self.id)
            self.directory = None
            #self.streams.clear()

    def CreatePlace(self, context):
        with self.lock:
            if getattr(self, "directory", None):
                raise RuntimeError("can't create duplicate working directory")
            if getattr(self, "id", None):
                self.directory = os.path.join(context.packets_directory, self.id)
                os.makedirs(self.directory)
            while not getattr(self, "id", None):
                directory = tempfile.mktemp(dir=context.packets_directory, prefix="pck-")
                id = os.path.split(directory)[-1]
                if not (context.Scheduler.GetPacket(id) or os.path.isdir(directory)):
                    try:
                        os.makedirs(directory)
                        self.directory = directory
                        self.id = id
                    except OSError: #os.makedirs failed"
                        pass
            osspec.set_common_readable(self.directory)
            osspec.set_common_executable(self.directory)
            self._CreateLinks(context)
            #self.streams = dict()

    def ListFiles(self):
        files = []
        with self.lock:
            if self.directory:
                try:
                    files = os.listdir(self.directory)
                except Exception, e:
                    logging.exception("directory %s listing error", self.directory)
        return files

    def GetFile(self, filename):
        with self.lock:
            if not self.directory:
                raise RuntimeError("working directory doesn't exist")
            path = os.path.join(self.directory, filename)
            if not os.path.isfile(path):
                raise AttributeError("not existing file: %s" % filename)
            if os.path.dirname(path) != self.directory:
                raise AttributeError("file %s is outside working directory" % filename)
            file = open(path, "r")

        with file:
            return file.read()

    def _create_job_file_handles(self, job):
        job.input = self._createInput(job.id)
        job.output = self._createOutput(job.id)

    def _apply_job_result_inner(self, job, result):
        new_state, delay = None, 0

        if result.IsCancelled()
            raise NotImplementedError()
        elif self.state in (PacketState.NONINITIALIZED, PacketState.SUSPENDED):
            self.leafs.add(job.id)
        elif result is not None and result.IsSuccessfull():
            self.done.add(job.id)
            if self.job_done_indicator.get(job.id):
                self.job_done_indicator[job.id].Set()
            for nid in self.edges[job.id]:
                self.waitJobs[nid].remove(job.id)
                if not self.waitJobs[nid]:
                    self.leafs.add(nid)
            if len(self.done) == len(self.jobs):
                new_state = PacketState.SUCCESSFULL
            elif self.leafs and self.state != PacketState.WAITING:
                new_state = PacketState.PENDING
        elif result is None or result.CanRetry():
            self.leafs.add(job.id)
            new_state = PacketState.WAITING
            delay = getattr(job, "retry_delay", None) or job.ERR_PENALTY_FACTOR ** job.tries
            logging.debug("packet %s\twaiting for %s sec", self.name, delay)
        else:
            self.result = PackedExecuteResult(len(self.done), len(self.jobs))
            new_state = PacketState.ERROR

        return new_state, delay

#class _JobProxy(object):
    #def __init__(self, impl, run_check):
        #self._impl = impl
        #self._run_check = run_check

    #def Run(self):
        #if self._run_check(self):
            #self._impl.Run()

    #def Terminate(self):
        #self._impl.Terminate()


class _ActiveJobs(object):
    def __init__(self):
        self._active = {} # From GetJobToRun moment
        self._results = []
        self._lock = threading.Lock()
        self._empty = threading.Condition(self._lock)

    def wait_empty(self):
        if not self._active:
            return

        with self._lock:
            while self._active:
                self._empty.wait()

    def add(self, job):
        with self._lock:
            self._active[job.id] = job

    def on_done(self, job, result):
        with self._lock:
            self._active.pop(job.id)
            self._results.append((job, result))

            if not self._active:
                self._empty.notify_all()

    def cancel(self):
        for job in self._active.values():
            job.Terminate()

    def pop_results(self):
        with self._lock:
            ret, self._results = self._results, []
        return ret


raise RuntimeError("TODO:\n\t1. IsCancelled()\n\t2. Inputs\n\t3. Think again about _kill_jobs\n\t4. _update_jobs_dependencies usage\n")

class JobPacket(Unpickable(lock=PickableRLock,
                           jobs=dict,
                           edges=dict,
                           done=set,
                           leafs=set,
                           _active_jobs=lambda *args: _ActiveJobs(),
                           job_done_indicator=dict,
                           waitingDeadline=int,
                           waitTags=set,
                           binLinks=dict,
                           state=(str, PacketState.CREATED),
                           history=(list, []),
                           notify_emails=(list, []),
                           flags=int,
                           kill_all_jobs_on_error=(bool, True),
                           as_in_queue_working=(set, []), # Need to be consistent with Queue.working
                           _clean_state=(bool, False), # False for loading old backups
                           isResetable=(bool, True)),
                CallbackHolder,
                ICallbackAcceptor,
                JobPacketImpl):
    INCORRECT = -1

    def __init__(self, name, priority, context, notify_emails, wait_tags=(), set_tag=None, kill_all_jobs_on_error=True, isResetable=True):
        super(JobPacket, self).__init__()
        self.name = name
        self.state = PacketState.NONINITIALIZED
        self._init(context)
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.id = os.path.split(self.directory)[-1]
        self.history.append((self.state, time.time()))
        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.isResetable = isResetable
        self.done_indicator = set_tag
        self._SetWaitingTags(wait_tags)

    def __getstate__(self):
# FIXME Use lock for non-fork backup?
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_indicator']:
            sdict['done_indicator'] = sdict['done_indicator'].name

        job_done_indicator = sdict['job_done_indicator'] = sdict['job_done_indicator'].copy()
        for job_id, tag in job_done_indicator.iteritems():
            job_done_indicator[job_id] = tag.name

        sdict.pop('waitingTime', None) # obsolete
        sdict.pop('_working_empty', None) # obsolete

        return sdict

    def _init(self, context):
        logging.info("packet init: %r %s", self, self.state)
        self._clean_state = True
        self.result = None
        if not getattr(self, "directory", None):
            self.CreatePlace(context)
        self.changeState(PacketState.CREATED)

    def __repr__(self):
        return "<JobPacket(id: %s; name: %s; state: %s)>" \
            % (getattr(self, "id", None), getattr(self, "name", None), getattr(self, "state", None))

    def _stream_file(self, jid, type):
        #stream = self.streams.get((type, jid), None)
        #if stream is not None:
            #if not stream.closed:
                #stream.close()
            #if stream.name != "<uninitialized file>":
                #return stream.name
        filename = os.path.join(self.directory, "%s-%s" % (type, jid))
        if os.path.isfile(filename):
            return filename
        return None

    def _createInput(self, jid):
        if jid in self.jobs:
            #filename = self._stream_file(jid, "in")
            job = self.jobs[jid]
            #if filename is not None:
                #stream = self.streams[("in", jid)] = open(filename, "r")
            if len(job.inputs) == 0:
                stream = self.streams[("in", jid)] = osspec.get_null_input()
            elif len(job.inputs) == 1:
                pid, = job.inputs
                logging.debug("pid: %s, pck_id: %s", pid, self.id)
                stream = self.streams[("in", jid)] = open(self._stream_file(pid, "out"), "r")
            else:
                with open(os.path.join(self.directory, "in-%s" % jid), "w") as writer:
                    for pid in job.inputs:
                        with open(self.streams[("out", pid)].name, "r") as reader:
                            writer.write(reader.read())
                stream = self.streams[("in", jid)] = open(writer.name, "r")
            return stream
        raise RuntimeError("alien job input request")

    def _createOutput(self, jid):
        if jid in self.jobs:
            filename = self._stream_file(jid, "out")
            if filename is None:
                filename = os.path.join(self.directory, "out-%s" % jid)
            stream = self.streams[("out", jid)] = open(filename, "w")
            return stream
        raise RuntimeError("alien job output request")

    def canChangeState(self, state):
        return state in PacketState.allowed[self.state]

    def stopWaiting(self):
        with self.lock:
            if self.state == PacketState.WAITING:
                self.changeState(PacketState.PENDING)

    def changeState(self, state):
        with self.lock:
            if state == self.state:
                return

            if not self.canChangeState(state):
                logging.warning("packet %s\tincorrect state change request %r => %r" % (self.name, self.state, state))
                return

            self.state = state
            self.history.append((self.state, time.time()))
            logging.debug("packet %s\tnew state %r", self.name, self.state)

            self.FireEvent("change") # queue.relocatePacket

            if state == PacketState.ERROR:
                self._kill_jobs()
                self._drop_jobs_results()
                self._send_email_on_error_state_if_need()

            if state == PacketState.SUCCESSFULL:
                if self.done_indicator:
                    self.done_indicator.Set()

            if state in [PacketState.SUCCESSFULL, PacketState.HISTORIED] or \
                    self.directory and not os.path.isdir(self.directory):
                self.ReleasePlace()

    def _send_email_on_error_state_if_need(self):
        if not self.notify_emails:
            return

        ctx = self._get_scheduler_ctx()
        if not ctx.send_emails:
            return
        if not ctx.send_emergency_emails and self.CheckFlag(PacketFlag.RCVR_ERROR):
            return

        msg = messages.FormatPacketErrorStateMessage(self, ctx)
        ctx.send_email_async(self.notify_emails, msg)

    def RegisterEmergency(self):
        if not self.notify_emails:
            return

        ctx = self._get_scheduler_ctx()

        if ctx.send_emails and ctx.send_emergency_emails:
            with self.lock:
                msg = messages.FormatPacketEmergencyError(self, ctx)
            ctx.send_email_async(self.notify_emails, msg)

    def OnStart(self, ref):
        if isinstance(ref, Job):
            assert False

    def on_job_start(self, ref):
        with self.lock:
# TODO Move self.state reaction in changeState in some variant
            #if self.state not in (PacketState.WORKABLE, PacketState.PENDING, PacketState.WAITING) \
                    #or ref.id not in self.jobs \
            if False \
                    or self.waitJobs[ref.id] \ # TODO Check #1
                    or not self.directory: # TODO Check #2
                raise RuntimeError("not all conditions are met for starting job %s; waitJobs: %s; packet state: %s; directory: %s" % ( ref.id, self.waitJobs[ref.id], self.state, self.directory))

            logging.debug("job %s\tstarted", ref.shell)

            #self._create_job_file_handles(ref)

    def OnUndone(self, ref):
        pass

    # Called in queue under packet lock
    def _get_working_jobs(self):
        return [self.jobs[jid] for jid in list(self.as_in_queue_working)]

    def on_job_done(self, job, result):
        self._active_jobs.on_done(job, result)

        # TODO Here this job may be run again
        # 1. _reset -> NONINITIALIZED -> WORKABLE
        # 2. WAITING -> WORKABLE
        # 3.

        with self.lock:
            self._apply_jobs_results()

    def _process_jobs_results(self, processor):
        for job, result in self._active_jobs.pop_results():
            logging.debug("job %s\tdone [%s]", job.shell, result)
            # FIXME Here or better somewhere between _apply_job_result_inner lines?
            self.as_in_queue_working.discard(job.id)
            self.FireEvent("job_done", job) # queue.working.discard(job)

            if processor:
                processor(job, result)

    def _apply_jobs_results(self):
        self._process_jobs_results(self._apply_job_result)

    def _drop_jobs_results(self):
        self._process_jobs_results(None)

    # this function called under lock and in "non-job-running" state
    def _kill_jobs(self):
        active = self._active_jobs

        active.cancel()
        active.wait_empty()

        self._close_streams() # FIXME Why this was first?

    def _apply_job_result(self, job, result):
        # FIXME This bullshit checks
        #if job.id in self.jobs and job.id in self.as_in_queue_working:

        new_state, retry_delay = self._apply_job_result_inner(job)
        if new_state:
            if new_state == PacketState.WAITING:
                self.waitingDeadline = time.time() + retry_delay
            self.changeState(new_state)

    def OnDone(self, ref):
        if isinstance(ref, Job):
            #self.on_job_done(ref)
            assert False
        elif isinstance(ref, TagBase):
            self._ProcessTagSetEvent(ref)

    def Add(self, shell, parents, pipe_parents, set_tag, tries,
            max_err_len, retry_delay, pipe_fail, description, notify_timeout, max_working_time, output_to_status):

        with self.lock:
            if self.state not in (PacketState.CREATED, PacketState.SUSPENDED):
                raise RuntimeError("incorrect state for \"Add\" operation: %s" % self.state)

            parents = list(set(p.id for p in parents + pipe_parents))
            pipe_parents = list(p.id for p in pipe_parents)
            job = Job(shell, parents, pipe_parents, self, maxTryCount=tries,
                      limitter=None, max_err_len=max_err_len, retry_delay=retry_delay,
                      pipe_fail=pipe_fail, description=description, notify_timeout=notify_timeout, max_working_time=max_working_time, output_to_status=output_to_status)
            self.jobs[job.id] = job
            if set_tag:
                self.job_done_indicator[job.id] = set_tag
            self.edges[job.id] = []
            for p in parents:
                self.edges[p].append(job.id)
            return job

    def _update_jobs_dependencies(self):
        def visit(startJID):
            st = [[startJID, 0]]
            discovered.add(startJID)

            while st:
                # num - number of neighbours discovered
                jid, num = st[-1]
                adj = self.edges[jid]
                if num < len(adj):
                    st[-1][1] += 1
                    nid = adj[num]
                    self.waitJobs[nid].add(jid)
                    if nid not in discovered:
                        discovered.add(nid)
                        st.append([nid, 0])
                    elif nid not in finished:
                        raise RuntimeError("job dependencies cycle exists")
                else:
                    st.pop()
                    finished.add(jid)

        with self.lock:
            discovered = set()
            finished = set()
            self.waitJobs = dict((jid, set()) for jid in self.jobs if jid not in self.done)
            for jid in self.jobs:
                if jid not in discovered and jid not in self.done:
                    visit(jid)
            #reset tries count for discovered jobs
            for jid in discovered:
                self.jobs[jid].tries = 0
            self.leafs = set(jid for jid in discovered if not self.waitJobs[jid])

    # XXX resumeWorkable is for backup loading
    def Resume(self, resumeWorkable=False):
        with self.lock:
            allowed_states = [PacketState.CREATED, PacketState.SUSPENDED]
            if resumeWorkable:
                allowed_states.append(PacketState.WORKABLE)
            if self.state in allowed_states and not self.CheckFlag(PacketFlag.USER_SUSPEND):
                self._clear_flag(~0)
                if self.waitTags:
                    self.changeState(PacketState.SUSPENDED)
                else:
                    self._update_jobs_dependencies() # FIXME XXX Does this destroy some in case Suspend(False); Resume
                    self.changeState(PacketState.WORKABLE)
                    if self.leafs:
                        self.changeState(PacketState.PENDING)
                    elif len(self.done) == len(self.jobs):
                        self.changeState(PacketState.SUCCESSFULL)

    def GetJobToRun(self):
        with self.lock:
    # FIXME Why not only PENDING
            if self.state not in [PacketState.WORKABLE, PacketState.PENDING]:
                return self.INCORRECT

            # FIXME moved from on_job_start
            if not hasattr(self, "waitJobs"):
                self._update_jobs_dependencies()

            ret = None

            for jid in self.leafs:
                job = self.jobs[jid]
                if job.CanStart():
                    self.FireEvent("job_get", job) # queue.working.add(job)
                    self.as_in_queue_working.add(jid)
                    self.leafs.remove(jid)
                    self._clean_state = False # not 100% accurate
                    ret = job
                    break

            if not self.leafs:
                self.changeState(PacketState.WORKABLE)

            ret._should_stop = False # hack to fix race TODO Kosher
            self._active_jobs.add(ret)

            return ret

    def IsDone(self):
        return self.state in (PacketState.SUCCESSFULL, PacketState.HISTORIED, PacketState.ERROR)

    def History(self):
        return self.history or []

# FIXME It's better for debug to allow this call from RPC without lock
#       * From messages it's called under lock actually
    def Status(self):
        #with self.lock:
        return self._Status()

    def _Status(self):
        history = self.History()
        total_time = history[-1][1] - history[0][1]
        wait_time = 0

        for ((state, start_time), (_, end_time)) in zip(history, history[1:] + [("", time.time())]):
            if state in (PacketState.SUSPENDED, PacketState.WAITING):
                wait_time += end_time - start_time

        result_tag = self.done_indicator.name if self.done_indicator else None

        waiting_time = max(int(self.waitingDeadline - time.time()), 0) \
            if self.state == PacketState.WAITING else None

        all_tags = list(getattr(self, 'allTags', []))

        status = dict(name=self.name,
                      state=self.state,
                      wait=list(self.waitTags),
                      all_tags=all_tags,
                      result_tag=result_tag,
                      priority=self.priority,
                      history=history,
                      total_time=total_time,
                      wait_time=wait_time,
                      last_modified=history[-1][1],
                      waiting_time=waiting_time)
        extra_flags = set()
        if self.state == PacketState.ERROR and self.CheckFlag(PacketFlag.RCVR_ERROR):
            extra_flags.add("can't-be-recovered")
        if self.CheckFlag(PacketFlag.USER_SUSPEND):
            extra_flags.add("manually-suspended")
        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

        if self.state in (PacketState.ERROR, PacketState.SUSPENDED,
                          PacketState.WORKABLE, PacketState.PENDING,
                          PacketState.SUCCESSFULL, PacketState.WAITING):
            status["jobs"] = []
            for jid, job in self.jobs.iteritems():
                result = job.Result()
                results = []
                if result:
                    results = [safeStringEncode(str(res)) for res in job.results]

                state = "done" if jid in self.done \
                    else "working" if jid in self.as_in_queue_working \
                    else "pending" if jid in self.leafs \
                    else "errored" if result and not result.IsSuccessfull() \
                    else "suspended"

                wait_jobs = []
                if self.state == PacketState.WORKABLE:
                    wait_jobs = map(str, self.waitJobs.get(jid, []))

                parents = map(str, job.parents or [])
                pipe_parents = map(str, job.inputs or [])

                output_filename = None
                if getattr(job, 'output', None) and os.path.isfile(job.output.name):
                    output_filename = os.path.basename(job.output.name)


                status["jobs"].append(
                    dict(id=str(job.id),
                         shell=job.shell,
                         desc=job.description,
                         state=state,
                         results=results,
                         parents=parents,
                         pipe_parents=pipe_parents,
                         output_filename=output_filename,
                         wait_jobs=wait_jobs,
                     )
                )
        return status

    def AddBinary(self, binname, file):
        with self.lock:
            self._AddLink(binname, file)

    def CheckFlag(self, flag):
        #return True if (self.flags & flag) else False
        return bool(self.flags & flag)

    def _SetFlag(self, flag):
        self.flags |= flag

    def MarkAsFailedOnRestore(self):
        with self.lock:
            self._SetFlag(PacketFlag.RCVR_ERROR)
            self.changeState(PacketState.ERROR)

    def _clear_flag(self, flag):
        self.flags &= ~flag

    def UserSuspend(self, kill_jobs=False):
        with self.lock:
            self._SetFlag(PacketFlag.USER_SUSPEND)
            self.Suspend(kill_jobs)

    def Suspend(self, kill_jobs=False):
        with self.lock:
            self.changeState(PacketState.SUSPENDED)
            if kill_jobs:
                self._kill_jobs()
                self._apply_jobs_results()

    def UserResume(self):
        with self.lock:
            self._clear_flag(PacketFlag.USER_SUSPEND)
            self.Resume()

    def _close_streams(self):
        try:
            for key, stream in self.streams.iteritems():
                if stream is not None:
                    if isinstance(stream, file):
                        if not stream.closed:
                            try:
                                stream.close()
                            except:
                                pass
        except:
            logging.exception('Close stream error')

    def _reset(self, suspend, tag_op):
        def update_tags():
    # FIXME Better always Reset tags
            for tag in self._get_all_done_tags():
                tag_op(tag)

        if self._clean_state and self.jobs: # force change state for packets without jobs
            # 1. Must put tag-change request into "queue" under JobPacket.lock
            # 2. Actual tag modification and reactions on it should be done async
            update_tags()
            return

        self.changeState(PacketState.NONINITIALIZED)
        self._kill_jobs()
        self._drop_jobs_results()

        update_tags()

        self.done.clear()
        for job in self.jobs.values():
            job.results = []

        self.Reinit(self._get_scheduler_ctx(), suspend)

    def Reinit(self, ctx, suspend=False): # FIXME Lock
        self._init(ctx)

        if self.CheckFlag(PacketFlag.USER_SUSPEND) or suspend:
            self.changeState(PacketState.SUSPENDED)
        else:
            self.Resume()

    def Reset(self, suspend=False):
        with self.lock:
            self._reset(suspend, lambda tag: tag.Unset())

    def _get_scheduler_ctx(self):
        return PacketCustomLogic.SchedCtx
        #return self._get_scheduler().context

    def _get_all_done_tags(self):
        if self.done_indicator:
            yield self.done_indicator

        for tag in self.job_done_indicator.itervalues():
            yield tag

        #for job_id in self.done: # FIXME job_done_indicator.values()
            #tag = self.job_done_indicator.get(job_id)
            #if tag:
                #yield tag

    def OnReset(self, (ref, comment)):
        if isinstance(ref, TagBase):
            self._on_tag_reset(ref, comment)

    def _send_reset_notification_if_need(self, comment):
        if not self.notify_emails:
            return
        ctx = self._get_scheduler_ctx()
        msg = messages.FormatPacketResetNotificationMessage(self, ctx, comment)
        ctx.send_email_async(self.notify_emails, msg)

    def SendJobLongExecutionNotification(self, job):
        if not self.notify_emails:
            return
        ctx = self._get_scheduler_ctx()
        with self.lock:
            msg = messages.FormatLongExecutionWarning(job, ctx)
        ctx.send_email_async(self.notify_emails, msg)

    def _on_tag_reset(self, ref, comment):
        with self.lock:
            self.waitTags.add(ref.GetFullname())

            if not self.isResetable:
                self._send_reset_notification_if_need(comment)
                return

            self._reset(False, lambda tag: tag.Reset(comment))

    def _move_to_queue(self, src_queue, dst_queue):
        with self.lock:
            if self.state not in [PacketState.CREATED, PacketState.SUSPENDED, PacketState.ERROR]:
                raise RuntimeError("can't move 'live' packet between queues")

            if src_queue:
                src_queue._detach_packet(self)
            if dst_queue:
                dst_queue._attach_packet(self)
                self.Resume() # FIXME

# Hack to restore from old backups (before refcatoring), when JobPacket was in
import job

job.JobPacket = JobPacket
