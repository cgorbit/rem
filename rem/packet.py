from __future__ import with_statement
import logging
import tempfile
import os
import time
import shutil
import errno
import threading

from callbacks import CallbackHolder, ICallbackAcceptor, TagBase, tagset
from common import BinaryFile, PickableRLock, Unpickable, safeStringEncode, as_rpc_user_error, RpcUserError
from job import Job, JobRunner
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
    WAITING = "WAITING"                 # wait timeout for retry failed jobs (may have running jobs)
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


def reraise(msg):
    t, e, tb = sys.exc_info()
    raise RuntimeError, RuntimeError('%s: %s' % (msg, e)), tb


class JobPacketImpl(object):
    """tags manipulation methods"""

    def _set_waiting_tags(self, wait_tags):
        for tag in wait_tags:
            tag.AddCallbackListener(self)
        self.allTags = set(tag.GetFullname() for tag in wait_tags)
        self.waitTags = set(tag.GetFullname() for tag in wait_tags if not tag.IsLocallySet())

    def _process_tag_set_event(self, tag):
        with self.lock:
            tagname = tag.GetFullname()
            if tagname in self.waitTags:
                self.waitTags.remove(tagname)
                if not self.waitTags and self.state == PacketState.SUSPENDED:
                    self._resume()

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
                    self._process_tag_set_event(tag)

    def _release_links(self):
        tmpLinks, self.binLinks = self.binLinks, {}
        while tmpLinks:
            binname, file = tmpLinks.popitem()
            if isinstance(file, BinaryFile):
                # until race condition in _change_state will be fixed FIXME it's fixed
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

    def _create_link(self, binname, file):
        file.Link(self, binname)
        self.binLinks[binname] = file

    def _add_link(self, binname, file):
        if binname in self.binLinks:
            old_file = self.binLinks.pop(binname)
            if isinstance(old_file, BinaryFile):
                old_file.Unlink(self, binname)
        self._create_link(binname, file)

    def _vivify_link(self, context, link):
        if isinstance(link, str):
            link = context.Scheduler.binStorage.GetFileByHash(link)
        elif isinstance(link, BinaryFile):
            link = context.Scheduler.binStorage.GetFileByHash(link.checksum)
        return link

    def _create_links(self, context):
        tmpLinks, self.binLinks = self.binLinks, {}
        while tmpLinks:
            binname, link = tmpLinks.popitem()
            file = self._vivify_link(context, link)
            if file is not None:
                self._create_link(binname, file)

    def _are_links_alive(self, context):
        return all(self._vivify_link(context, link) for link in self.binLinks.itervalues())

    def _release_place(self):
        with self.lock:
            # 1. ERROR may contains running jobs in kill_all_jobs_on_error=False
            # 2. SUSPENDED may contains running jobs
            # 3. {ERROR, SUSPENDED} may become HISTORIED
            if self._active_jobs:
                logging.error("_release_place of %s in %s with active jobs" % (self.id, self.state))
            self._kill_jobs_drop_results()

            self._release_links()

            if self.directory and os.path.isdir(self.directory):
                try:
                    shutil.rmtree(self.directory, onerror=None)
                except Exception, e:
                    logging.exception("Packet %s release place error", self.id)

            self.directory = None
            self.streams.clear()

    def _create_place(self, context):
        if self.directory:
            raise RuntimeError("can't create duplicate working directory")

        if self.id:
            self.directory = os.path.join(context.packets_directory, self.id)
            os.makedirs(self.directory)

        while True:
            directory = tempfile.mktemp(dir=context.packets_directory, prefix="pck-")
            id = os.path.split(directory)[-1]
            if not (context.Scheduler.GetPacket(id) or os.path.isdir(directory)): # race
                try:
                    os.makedirs(directory)
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        continue
                    raise
                else:
                    self.directory = directory
                    self.id = id
                    break

        osspec.set_common_readable(self.directory)
        osspec.set_common_executable(self.directory)
        self._create_links(context)
        self.streams.clear()

    def rpc_list_files(self):
        files = []
        with self.lock:
            if self.directory:
                try:
                    files = os.listdir(self.directory)
                except Exception, e:
                    logging.exception("directory %s listing error", self.directory)
        return files

    def rpc_get_file(self, filename):
        with self.lock:
            if not self.directory:
                raise RpcUserError(RuntimeError("working directory doesn't exist"))
            path = os.path.join(self.directory, filename)
            if not os.path.isfile(path):
                raise AttributeError("not existing file: %s" % filename)
            if os.path.dirname(path) != self.directory:
                raise AttributeError("file %s is outside working directory" % filename)
            file = open(path, "r")

        with file:
            return file.read()

    def _apply_job_result(self, job, runner):
        # TODO
        # 1. kill_all_jobs_on_error=False doesn't work correctly:
        #       Another running jobs may change ERROR to PENDING
        # 2. WAITING doesn't work as good as possible:
        #       1. if another jobs are running state must still be WORKABLE/PENDING,
        #          and only later become WAITING (or never WAITING)
        #       2. another running jobs' branches will not be continue to run
        #          until stopWaiting()

        if runner.returncode == 0 and not runner.is_cancelled():
            self.done.add(job.id)

            if self.job_done_indicator.get(job.id):
                self.job_done_indicator[job.id].Set()

            for nid in self.edges[job.id]:
                self.waitJobs[nid].remove(job.id)
                if not self.waitJobs[nid]:
                    self.leafs.add(nid)

            if len(self.done) == len(self.jobs):
                self._change_state(PacketState.SUCCESSFULL)

            elif self.leafs and self.state != PacketState.WAITING:
                self._change_state(PacketState.PENDING)

        elif job.tries < job.maxTryCount:
            self.leafs.add(job.id)

            delay = job.retry_delay or job.ERR_PENALTY_FACTOR ** job.tries
            self.waitingDeadline = time.time() + delay
            self._change_state(PacketState.WAITING)
            logging.debug("packet %s\twaiting for %s sec", self.name, delay)

        else:
            self.leafs.add(job.id)

            self._change_state(PacketState.ERROR)

            if self.kill_all_jobs_on_error:
                self._kill_jobs_drop_results()


class _ActiveJobs(object):
    def __init__(self):
        self._active = {} # From GetJobToRun moment
        self._results = []
        self._lock = threading.Lock()
        self._empty = threading.Condition(self._lock)

    def __nonzero__(self):
        return bool(len(self))

    # FIXME Non-obvious meaning?
    def __len__(self):
        with self._lock:
            return len(self._active) + len(self._results)

    def wait_empty(self):
        if not self._active:
            return

        with self._lock:
            while self._active:
                self._empty.wait()

    def add(self, runner):
        with self._lock:
            job_id = runner.job.id
            #logging.debug('_active[%d] = %s' % (job_id, runner.job))
            assert job_id not in self._active, "Job %d already in _active" % job_id
            self._active[job_id] = runner

    def on_done(self, runner):
        with self._lock:
            job_id = runner.job.id
            #logging.debug('_active.pop(%d)' % job_id)
            assert job_id in self._active, "No Job %d in _active" % job_id
            self._active.pop(job_id)
            self._results.append(runner)

            if not self._active:
                self._empty.notify_all()

    def terminate(self):
        for runner in self._active.values():
            runner.cancel()

    def pop_results(self):
        with self._lock:
            ret, self._results = self._results, []
        return ret


def always(ctor):
    def always(*args):
        return ctor()
    return always


class JobPacket(Unpickable(lock=PickableRLock,
                           jobs=dict,
                           edges=dict,
                           done=set,
                           leafs=set,
                           _active_jobs=always(_ActiveJobs),
                           job_done_indicator=dict,
                           waitingDeadline=int,
                           allTags=set,
                           waitTags=set,
                           binLinks=dict,
                           state=(str, PacketState.ERROR),
                           history=list,
                           notify_emails=list,
                           flags=int,
                           kill_all_jobs_on_error=(bool, True),
                           _clean_state=(bool, False), # False for loading old backups
                           isResetable=(bool, True),
                           directory=lambda *args: args[0] if args else None,

                           # FIXME equal to _active_jobs.{_active + _results}?
                           # Need to be consistent with Queue.working under JobPacket.lock
                           as_in_queue_working=always(set),
                          ),
                CallbackHolder,
                ICallbackAcceptor,
                JobPacketImpl):

    INCORRECT = -1

    # FIXME Why WORKABLE too?
    # Actually WAITING and ERROR sometimes applicable too run jobs (see _apply_job_result)
    _ALLOWED_TO_RUN_NEW_JOBS_STATES = [PacketState.WORKABLE, PacketState.PENDING]

    def __init__(self, name, priority, context, notify_emails, wait_tags=(), set_tag=None, kill_all_jobs_on_error=True, isResetable=True):
        super(JobPacket, self).__init__()
        self.name = name
        self.state = PacketState.NONINITIALIZED
        self.history.append((self.state, time.time()))
        self.id = None
        self.directory = None
        self.streams = {}
        self.waitJobs = {}
        self._init(context)
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.id = os.path.split(self.directory)[-1]
        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.isResetable = isResetable
        self.done_indicator = set_tag
        self._set_waiting_tags(wait_tags)

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_indicator']:
            sdict['done_indicator'] = sdict['done_indicator'].name

        job_done_indicator = sdict['job_done_indicator'] = sdict['job_done_indicator'].copy()
        for job_id, tag in job_done_indicator.iteritems():
            job_done_indicator[job_id] = tag.name

        sdict.pop('waitingTime', None) # obsolete
        sdict.pop('_working_empty', None) # obsolete
        sdict.pop('_active_jobs', None)

        return sdict

    def _init(self, context):
        logging.info("packet init: %r %s", self, self.state)
        self._clean_state = True
        if self.directory is None:
            self._create_place(context)
        self._change_state(PacketState.CREATED)

    def __repr__(self):
        return "<JobPacket(id: %s; name: %s; state: %s)>" % (self.id, self.name, self.state)

    # TODO Don't store filehandles in self, only filenames

    def _stream_file(self, jid, type):
        stream = self.streams.get((type, jid), None)
        if stream is not None:
            if not stream.closed:
                stream.close()
            if stream.name != "<uninitialized file>":
                return stream.name
        filename = os.path.join(self.directory, "%s-%s" % (type, jid))
        if os.path.isfile(filename):
            return filename
        return None

    def _create_input(self, jid):
        if jid in self.jobs:
            filename = self._stream_file(jid, "in")
            job = self.jobs[jid]
            if filename is not None:
                stream = self.streams[("in", jid)] = open(filename, "r")
            elif len(job.inputs) == 0:
                stream = self.streams[("in", jid)] = osspec.get_null_input()
            elif len(job.inputs) == 1:
                pid, = job.inputs
                logging.debug("pid: %s, pck_id: %s", pid, self.id)
                stream = self.streams[("in", jid)] = open(self._stream_file(pid, "out"), "r")
            else:
                with open(os.path.join(self.directory, "in-%s" % jid), "w") as writer:
                    for pid in job.inputs:
                        with open(self.streams[("out", pid)].name, "r") as reader:
                            writer.write(reader.read()) # TODO chunks
                stream = self.streams[("in", jid)] = open(writer.name, "r")
            return stream
        raise RuntimeError("alien job input request")

    def _create_output(self, jid, type):
        if jid in self.jobs:
            filename = self._stream_file(jid, type)
            if filename is None:
                filename = os.path.join(self.directory, "%s-%s" % (type, jid))
            stream = self.streams[(type, jid)] = open(filename, "w")
            return stream
        raise RuntimeError("alien job output request")

    # We can't use lock here (deadlock)
    # FIXME We can avoid locking here because call to _kill_jobs() in _release_place()
    # garantee that pck.directory will exists
    def _create_job_file_handles(self, job):
        return (
            self._create_input(job.id),
            self._create_output(job.id, "out"),
            self._create_output(job.id, "err")
        )

    def _can_change_state(self, state):
        return state in PacketState.allowed[self.state]

    def stopWaiting(self):
        with self.lock:
            if self.state == PacketState.WAITING:
                self._change_state(PacketState.PENDING)

    def _change_state(self, state):
        with self.lock:
            if state == self.state:
                logging.debug("packet %s useless state change to current %s" % (self.id, state))
                return

            assert self._can_change_state(state), \
                "packet %s\tincorrect state change request %r => %r" % (self.name, self.state, state)

            self.state = state
            self.history.append((self.state, time.time()))
            logging.debug("packet %s\tnew state %r", self.name, self.state)

            self.FireEvent("change") # queue.relocatePacket

            if state == PacketState.ERROR:
                self._try_send_email_on_error_state_if_need()

            if state == PacketState.SUCCESSFULL:
                if self.done_indicator:
                    self.done_indicator.Set()

            if state in [PacketState.SUCCESSFULL, PacketState.HISTORIED]:
                self._release_place() # will kill jobs if need

    def rpc_remove(self):
        with self.lock:
            new_state = PacketState.HISTORIED
            if self.state == new_state:
                return
            if not self._can_change_state(new_state):
                raise RpcUserError(RuntimeError("Can't remove packet in %s state" % self.state))
            self._change_state(new_state)

    def RemoveAsOld(self):
        with self.lock:
            if self.state == PacketState.CREATED: # hack for ShortStorage
                self._change_state(PacketState.SUSPENDED)
            self._change_state(PacketState.HISTORIED)

    def _mark_as_failed_on_recovery(self):
        with self.lock:
            if self.check_flag(PacketFlag.RCVR_ERROR) and self.state == PacketState.ERROR:
                return

            self._set_flag(PacketFlag.RCVR_ERROR)
            self._change_state(PacketState.ERROR)

            self._release_place()

    def _try_recover_directory(self, ctx):
        if os.path.isdir(self.directory):
            parentDir, dirname = os.path.split(self.directory)
            if parentDir != ctx.packets_directory:
                dst_loc = os.path.join(ctx.packets_directory, self.id)
                try:
                    logging.debug("relocates directory %s to %s", self.directory, dst_loc)
                    shutil.copytree(self.directory, dst_loc)
                    self.directory = dst_loc
                except:
                    reraise("Failed to relocate directory")

        else:
            if not self._are_links_alive(ctx):
                raise RuntimeError("Not all links alive")

            try:
                self.directory = None
                self._create_place(ctx)
            except:
                reraise("Failed to resurrect directory")

    def _try_recover_after_backup_loading(self, ctx):
        if self.state == PacketState.ERROR and self.check_flag(PacketFlag.RCVR_ERROR) \
                or self.state == PacketState.HISTORIED:
            if self.directory:
                self._release_place()
            return

        # Can exists only in tempStorage or in backups wo locks+backup_in_child
        if self.state == PacketState.CREATED:
            raise RuntimeError("Can't restore packets in CREATED state")

        if self.state == PacketState.NONINITIALIZED:
            self._recover_noninitialized(ctx)

        if self.directory:
            self._try_recover_directory(ctx)

        elif self.state != PacketState.SUCCESSFULL:
            # FIXME try: _create_place
            raise RuntimeError("No .directory set for packet in %s state" % self.state)

        self._init_job_deps_graph()
        self._resume(resume_workable=True, silent_noop=True) # TODO write tests

    def try_recover_after_backup_loading(self, ctx):
        descr = '[%s, directory = %s]' % (self, self.directory)

        try:
            self._try_recover_after_backup_loading(ctx)
        except Exception as e:
            logging.exception("Failed to recover packet %s" % descr)
            self._mark_as_failed_on_recovery()

    def _recover_noninitialized(self, ctx):
        dir = None
        if self.directory:
            dir = self.directory
        elif self.id:
            dir = os.path.join(ctx.packets_directory, self.id)
        else:
            raise RuntimeError("No .id in NONINITIALIZED packet %s" % self.id)

        self._release_place()
        self._reinit(ctx)

    def _notify_incorrect_action(self):
        if not self.notify_emails:
            return

        ctx = self._get_scheduler_ctx()

        if ctx.send_emails and ctx.send_emergency_emails:
            with self.lock:
                msg = messages.FormatPacketEmergencyError(self, ctx)
            ctx.send_email_async(self.notify_emails, msg)

    def _send_email_on_error_state_if_need(self):
        if not self.notify_emails:
            return

        ctx = self._get_scheduler_ctx()
        if not ctx.send_emails:
            return
        if not ctx.send_emergency_emails and self.check_flag(PacketFlag.RCVR_ERROR):
            return

        msg = messages.FormatPacketErrorStateMessage(self, ctx)
        ctx.send_email_async(self.notify_emails, msg)

    def _try_send_email_on_error_state_if_need(self):
        try:
            self._send_email_on_error_state_if_need()
        except:
            logging.exception("Failed to send error message for %s" % self.id)

    def OnUndone(self, ref):
        pass

    # Called in queue under packet lock
    def _get_working_jobs(self):
        return [self.jobs[jid] for jid in list(self.as_in_queue_working)]

    def on_job_done(self, runner):
        self._active_jobs.on_done(runner)

        with self.lock:
            self._apply_jobs_results()

    def _process_jobs_results(self, processor):
        for runner in self._active_jobs.pop_results():
            try:
                self._process_job_result(processor, runner)
            except Exception:
                logging.exception("Failed to process job result for %s" % runner.job)

    def _process_job_result(self, processor, runner):
        job = runner.job
        assert job.id in self.jobs
        logging.debug("job %s\tdone [%s]", job.shell, job.last_result())
        self.as_in_queue_working.discard(job.id) # FIXME Here or better somewhere between _apply_job_result lines?
        self.FireEvent("job_done", job) # queue.working.discard(job)
        if processor:
            processor(job, runner)

    def _apply_jobs_results(self):
        self._process_jobs_results(self._apply_job_result)

    def _drop_jobs_results(self):
        def revert_leaf(job, runner):
            self.leafs.add(job.id)
        self._process_jobs_results(revert_leaf)

    def _kill_jobs(self):
        if self.state in self._ALLOWED_TO_RUN_NEW_JOBS_STATES:
            logging.error("_kill_jobs on %s in %s" % (self.id, self.state))

        active = self._active_jobs

        active.terminate()
        active.wait_empty()

        self._close_streams()

    def _kill_jobs_drop_results(self):
        self._kill_jobs()
        self._drop_jobs_results()

    def OnDone(self, ref):
        if isinstance(ref, TagBase):
            self._process_tag_set_event(ref)

    def rpc_add_job(self, shell, parents, pipe_parents, set_tag, tries,
            max_err_len, retry_delay, pipe_fail, description, notify_timeout,
            max_working_time, output_to_status):

        with self.lock:
            # Actually, pck_add_job use only tempStorage, so SUSPENDED is not used
            if self.state not in [PacketState.CREATED, PacketState.SUSPENDED]:
                raise RpcUserError(RuntimeError("incorrect state for \"Add\" operation: %s" % self.state))

            if self.state == PacketState.SUSPENDED:
                logging.warning("JobPacket.rpc_add_job for %s in SUSPENDED" % self.id)

            parents = list(set(p.id for p in parents + pipe_parents))
            pipe_parents = list(p.id for p in pipe_parents)

            for dep_id in parents:
                if dep_id not in self.jobs:
                    raise RpcUserError(RuntimeError("No job with id = %s in packet %s" % (dep_id, self.id)))

            job = Job(shell, parents, pipe_parents, self, maxTryCount=tries,
                      max_err_len=max_err_len, retry_delay=retry_delay,
                      pipe_fail=pipe_fail, description=description, notify_timeout=notify_timeout, max_working_time=max_working_time, output_to_status=output_to_status)

            self.jobs[job.id] = job

            self._add_job_to_graph(job)

            if set_tag:
                self.job_done_indicator[job.id] = set_tag

            return job

    def _add_job_to_graph(self, job):
        # waitJobs[child] -> parents
        # edges[parent]   -> children (constant between calls to Add)

        parents = job.parents

        self.edges[job.id] = []
        for p in parents:
            self.edges[p].append(job.id)

        self.waitJobs[job.id] = [jid for jid in parents if jid not in self.done]

        if not self.waitJobs[job.id]:
            self.leafs.add(job.id)

    def _init_job_deps_graph(self):
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
            assert not self._active_jobs, "_init_job_deps_graph called when _active_jobs is not empty"

            discovered = set()
            finished = set()

            self.waitJobs = {jid: set() for jid in self.jobs if jid not in self.done}

            for jid in self.jobs:
                if jid not in discovered and jid not in self.done:
                    visit(jid)

            self.leafs = set(jid for jid in discovered if not self.waitJobs[jid])

    # FIXME Diese Funktion ist Wunderwaffe
    def _resume(self, resume_workable=False, silent_noop=False):
        allowed_states = [PacketState.CREATED, PacketState.SUSPENDED]
        if resume_workable:
            allowed_states.append(PacketState.WORKABLE)

        if self.state in allowed_states and not self.check_flag(PacketFlag.USER_SUSPEND):
            self._clear_flag(~0)

            if self.waitTags:
                new_state = PacketState.SUSPENDED
            else:
                if self.leafs:
                    new_state = PacketState.PENDING
                elif len(self.done) == len(self.jobs):
                    new_state = PacketState.SUCCESSFULL
                else:
                    new_state = PacketState.WORKABLE

            if silent_noop and new_state == self.state:
                return

            if new_state in [PacketState.PENDING, PacketState.SUCCESSFULL]:
                self._change_state(PacketState.WORKABLE)

            self._change_state(new_state)

    def GetJobToRun(self):
        with self.lock:
            if self.state not in self._ALLOWED_TO_RUN_NEW_JOBS_STATES:
                return self.INCORRECT

            def get_job():
                for jid in self.leafs:
                    job = self.jobs[jid]
                    self.FireEvent("job_get", job) # queue.working.add(job)
                    self.as_in_queue_working.add(jid)
                    self.leafs.remove(jid)
                    return job

            job = get_job()

            if not job:
                if self.state == PacketState.PENDING:
                    self._change_state(PacketState.ERROR)
                    self._kill_jobs_drop_results()
                    self._notify_incorrect_action()
                return self.INCORRECT

            self._clean_state = False

            if not self.leafs:
                self._change_state(PacketState.WORKABLE)

            runner = JobRunner(job)
            self._active_jobs.add(runner)
            return runner

    def IsDone(self):
        return self.state in (PacketState.SUCCESSFULL, PacketState.HISTORIED, PacketState.ERROR)

    def History(self):
        return self.history or []

    # FIXME It's better for debug to allow this call from RPC without lock
    #       * From messages it's called under lock actually
    def Status(self):
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

        all_tags = list(self.allTags)

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
        if self.state == PacketState.ERROR and self.check_flag(PacketFlag.RCVR_ERROR):
            extra_flags.add("can't-be-recovered")
        if self.check_flag(PacketFlag.USER_SUSPEND):
            extra_flags.add("manually-suspended")
        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

        if self.state in (PacketState.ERROR, PacketState.SUSPENDED,
                          PacketState.WORKABLE, PacketState.PENDING,
                          PacketState.SUCCESSFULL, PacketState.WAITING):
            status["jobs"] = []
            for jid, job in self.jobs.iteritems():
                result = job.last_result()
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

    def rpc_add_binary(self, binname, file):
        with self.lock:
            self._add_link(binname, file)

    def CheckFlag(self, flag):
        return bool(self.flags & flag)

    check_flag = CheckFlag

    def _set_flag(self, flag):
        self.flags |= flag

    def _clear_flag(self, flag):
        self.flags &= ~flag

    def rpc_suspend(self, kill_jobs=False):
        with self.lock:
            if self.state == PacketState.ERROR and self.check_flag(PacketFlag.RCVR_ERROR):
                raise RpcUserError(
                    RuntimeError("Can't suspend ERROR'ed packet %s with RCVR_ERROR flag set" % self.id))
            self._set_flag(PacketFlag.USER_SUSPEND)
            self.Suspend(kill_jobs)

    def Suspend(self, kill_jobs=False):
        with self.lock:
            self._change_state(PacketState.SUSPENDED)

            if kill_jobs:
                # FIXME In ideal world it's better to "apply" jobs that will be
                # finished racy just before kill(2)
                self._kill_jobs_drop_results()

    def rpc_resume(self):
        with self.lock:
            # FIXME raise on non-SUSPENDED states?

            if self.state == PacketState.SUSPENDED:
                self._clear_flag(PacketFlag.USER_SUSPEND)

                # TODO write tests
                # Repeat legacy bullshit behaviour
                for job in self.jobs.values():
                    job.tries = 0

                self._resume()

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
            for tag in self._get_all_done_tags():
                tag_op(tag)

        # XXX force call to _change_state for packets without jobs
        #if self._clean_state and self.jobs:
            #update_tags()
            #return

        self._change_state(PacketState.NONINITIALIZED)

        if not self._clean_state:
            self._kill_jobs_drop_results()

            self.done.clear()
            for job in self.jobs.values():
                job.results = []
                job.tries = 0

            self._init_job_deps_graph()

        update_tags()

        self._reinit(self._get_scheduler_ctx(), suspend)

    def _reinit(self, ctx, suspend=False):
        self._init(ctx)

        if self.check_flag(PacketFlag.USER_SUSPEND) or suspend:
            self._change_state(PacketState.SUSPENDED)
        else:
            self._resume()

    def Reset(self, suspend=False):
        with self.lock:
            self._reset(suspend, lambda tag: tag.Unset())

    def _get_scheduler_ctx(self):
        return PacketCustomLogic.SchedCtx
        #return self._get_scheduler().context

    def _get_all_done_tags(self):
        return self.job_done_indicator.values() + \
            ([self.done_indicator] if self.done_indicator else [])

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

    def _move_to_queue(self, src_queue, dst_queue, from_rpc=False):
        with self.lock:
            if self.state not in [PacketState.CREATED, PacketState.SUSPENDED, PacketState.ERROR]:
                raise as_rpc_user_error(
                    from_rpc, RuntimeError("can't move 'live' packet between queues"))

            if src_queue:
                src_queue._detach_packet(self)

            if dst_queue:
                dst_queue._attach_packet(self)

    def rpc_move_to_queue(self, src_queue, dst_queue):
        self._move_to_queue(src_queue, dst_queue, from_rpc=True)

    def _attach_to_queue(self, queue):
        with self.lock:
            self._move_to_queue(None, queue)
            self._resume()
