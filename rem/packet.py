from __future__ import with_statement
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
from rem_logging import logger as logging
import delayed_executor

class ReprState(object):
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
    #allowed = {
        #CREATED: (WORKABLE, SUSPENDED),
        #WORKABLE: (SUSPENDED, ERROR, SUCCESSFULL, PENDING, WAITING, NONINITIALIZED),
        #PENDING: (SUSPENDED, WORKABLE, ERROR, WAITING, NONINITIALIZED),
        #SUSPENDED: (WORKABLE, HISTORIED, WAITING, ERROR, NONINITIALIZED),
        #WAITING: (PENDING, SUSPENDED, ERROR, NONINITIALIZED),
        #ERROR: (SUSPENDED, HISTORIED, NONINITIALIZED),
        #SUCCESSFULL: (HISTORIED, NONINITIALIZED),
        #NONINITIALIZED: (CREATED,),
        #HISTORIED: ()}


class ImplState(object):
    UNINITIALIZED = '_UNINITIALIZED' # jobs, files and queue not set
    ACTIVE      = '_ACTIVE'
    SUCCESSFULL = '_SUCCESSFULL'
    ERROR       = '_ERROR'
    BROKEN      = '_BROKEN'
    #WAITING_JOBS_CANCELLATION = '_WAITING_JOBS_CANCELLATION' # actual for sandbox task reset
    HISTORIED   = '_HISTORIED'

    allowed {
        UNINITIALIZED: [BROKEN, ACTIVE, HISTORIED],
        ACTIVE:        [BROKEN, SUCCESSFULL, ERROR], # + HISTORIED (see _can_change_state)
        SUCCESSFULL:   [BROKEN, ACTIVE, HISTORIED],
        ERROR:         [BROKEN, ACTIVE, ERROR],
        BROKEN:        [HISTORIED],
        HISTORIED:     [],
    }


################################################################################
# # XXX NOTHING means "only ReprState-and-subqueue change"
# ._impl_state
#     ACTIVE          -> potentially can start jobs
#     other           -> NOTHING
#
# .wait_dep_tags
#     OnDone
#         empty       -> potentially can start jobs
#     OnReset
#         not empty   -> NOTHING
# 
# .jobs_to_run
#     Timer for .jobs_to_retry
#         not empty   -> potentially can start jobs
#     Start job from .jobs_to_run
#         empty       -> NOTHING
# 
# .jobs_to_retry
#     Job failed and must reran
#         not empty   -> NOTHING
#     Timer
#         empty       -> ->jobs_to_run.add
# 
# .dont_run_new_jobs
#     User RPC call
#         True        -> NOTHING
#     User RPC call
#         False       -> potentially can start jobs
#
# .failed_jobs
#     TODO
#
#################################################################################


#class PacketFlag:
    #USER_SUSPEND = 0x01              #suspended by user
    #RCVR_ERROR = 0x02              #recovering error detected


class PacketCustomLogic(object):
    SchedCtx = None

    @classmethod
    def UpdateContext(cls, context):
        cls.SchedCtx = context


def reraise(msg):
    t, e, tb = sys.exc_info()
    raise RuntimeError, RuntimeError('%s: %s' % (msg, e)), tb


class _ActiveJobs(object):
    def __init__(self):
        self._running = {} # From GetJobToRun moment
        self._results = []
        self._lock = threading.Lock()
        self._empty = threading.Condition(self._lock)

    def __nonzero__(self):
        return bool(len(self))

    # FIXME Non-obvious meaning?
    def __len__(self):
        with self._lock:
            return len(self._running) + len(self._results)

    def has_results(self):
        return bool(self._results)

    def has_running(self):
        return bool(self._running)

    def wait_running_empty(self):
        if not self._running:
            return

        with self._lock:
            while self._running:
                self._empty.wait()

    def add(self, runner):
        with self._lock:
            job_id = runner.job.id
            #logging.debug('_running[%d] = %s' % (job_id, runner.job))
            assert job_id not in self._running, "Job %d already in _running" % job_id
            self._running[job_id] = runner

    def on_done(self, runner):
        with self._lock:
            job_id = runner.job.id
            #logging.debug('_running.pop(%d)' % job_id)
            assert job_id in self._running, "No Job %d in _running" % job_id
            self._running.pop(job_id)
            self._results.append(runner)

            if not self._running:
                self._empty.notify_all()

    def terminate(self):
        for runner in self._running.values():
            runner.cancel()

    def pop_result(self):
        return self._results.pop(0)


def always(ctor):
    def always(*args):
        return ctor()
    return always

#                                     ---jobs_to_retry--
#                                     |                 \
#                                     |                  |
#                                     |                  |
# jobs_graph -> wait_job_deps -> jobs_to_run -> active_jobs_cache
#                     ^                                  |
#                     |                                  |- failed_jobs
#                     |                                  |
#                     |                                  `- succeed_jobs
#                     |                                          |
#                     \-----------------------------------------/

class PacketBase(Unpickable(
                           lock=PickableRLock,
                           jobs=dict,
                           jobs_graph=dict,
                           wait_job_deps=dict,
                           succeed_jobs=set,
                           failed_jobs=set,
                           jobs_to_run=set,
                           jobs_to_retry=dict,
                           _active_jobs=always(_ActiveJobs),
                           job_done_tag=dict,
                           all_dep_tags=set,
                           wait_dep_tags=set,
                           bin_links=dict,
                           _repr_state=(str, PacketState.ERROR),
                           history=list,
                           notify_emails=list,
                           kill_all_jobs_on_error=(bool, True),
                           _clean_state=(bool, False), # False for loading old backups
                           is_resetable=(bool, True),
                           notify_on_reset=(bool, False),
                           notify_on_skipped_reset=(bool, True),
                           directory=lambda *args: args[0] if args else None,
                           active_jobs_cache=always(set),
                          ),
                CallbackHolder,
                ICallbackAcceptor):

    def __init__(self, name, priority, context, notify_emails, wait_tags=(),
                 set_tag=None, kill_all_jobs_on_error=True, is_resetable=True,
                 notify_on_reset=False, notify_on_skipped_reset=True):
        super(PacketBase, self).__init__()
        self.name = name
        #self._repr_state = PacketState.NONINITIALIZED
        #self.history.append((self._repr_state, time.time()))
        self.id = None
        self.directory = None
        self.streams = {} # TODO Remove this bullshit
        self.wait_job_deps = {}
        self._clean_state = True
        self._create_place_if_need(context)
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.id = os.path.split(self.directory)[-1]
        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.is_resetable = is_resetable
        self.done_tag = set_tag
        self._set_waiting_tags(wait_tags)

        self._impl_state = ImplState.UNINITIALIZED
        self._update_repr_state()

    def _calc_repr_state(self):
        state = self._impl_state

        if state == ImplState.UNINITIALIZED:
            return ReprState.CREATED

        elif state in [ImplState.ERROR, ImplState.BROKEN]:
            return ReprState.ERROR

        elif state == ImplState.HISTORIED:
            return ReprState.HISTORIED

        # XXX PacketFlag.USER_SUSPEND -> means -> .dont_run_new_jobs
        # XXX PacketFlag.RCVR_ERROR   -> means -> .state == BROKEN

        #type(.wait_dep_tags) == set # XXX FIXME We can push() Unset-tags on _reset()
        #type(.jobs_to_run) == set # contains non-WAITING jobs even on .dont_run_new_jobs
        #type(.jobs_to_retry) == dict
        #type(.dont_run_new_jobs) == bool
        #type(.failed_jobs) == set # kill_all_jobs_on_error == False

        has_active_jobs = bool(self.active_jobs_cache)

        if .wait_dep_tags:
            return ReprState.SUSPENDED

        elif .has_active_jobs:
            if .dont_run_new_jobs or self.failed_jobs:
                return ReprState.WORKABLE
            elif .jobs_to_run:
                return ReprState.PENDING
            else:
                return ReprState.WORKABLE

        elif self.failed_jobs:
            raise AssertionError("Unreachable") # reachable only on [not .has_active_jobs]

        elif .dont_run_new_jobs:
            return ReprState.SUSPENDED

        elif .jobs_to_run:
            return ReprState.PENDING

        elif .jobs_to_retry:
            return ReprState.WAITING

        else:
            raise AssertionError("Unreachable")

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_tag']:
            sdict['done_tag'] = sdict['done_tag'].name

        job_done_tag = sdict['job_done_tag'] = sdict['job_done_tag'].copy()
        for job_id, tag in job_done_tag.iteritems():
            job_done_tag[job_id] = tag.name

        sdict.pop('waitingTime', None) # obsolete
        sdict.pop('_working_empty', None) # obsolete
        sdict.pop('_active_jobs', None)
        sdict.pop('active_jobs_cache', None)

        sdict['jobs_to_retry'] = {
            job_id: deadline for job_id, cancel, deadline in sdict['jobs_to_retry'].values()
        }

        return sdict

    def _create_place_if_need(self, context):
        #logging.info("packet init: %r %s", self, self._repr_state) # FIXME
        if self.directory is None:
            self._create_place(context)

    def __repr__(self):
        return "<%s(id: %s; name: %s; state: %s)>" % (type(self).__name__, self.id, self.name, self._repr_state)

    def _set_waiting_tags(self, wait_tags):
        for tag in wait_tags:
            tag.AddCallbackListener(self)
        self.all_dep_tags = set(tag.GetFullname() for tag in wait_tags)
        self.wait_dep_tags = set(tag.GetFullname() for tag in wait_tags if not tag.IsLocallySet())

    def _process_tag_set_event(self, tag):
        tagname = tag.GetFullname()
        if tagname in self.wait_dep_tags:
            self.wait_dep_tags.remove(tagname)
            if not self.wait_dep_tags:
               self._update_repr_state()

    def vivify_done_tags_if_need(self, tagStorage):
        with self.lock:
            if isinstance(self.done_tag, str):
                self.done_tag = tagStorage.AcquireTag(self.done_tag)
            for jid, cur_val in self.job_done_tag.iteritems():
                if isinstance(cur_val, str):
                    self.job_done_tag[jid] = tagStorage.AcquireTag(cur_val)

    def vivify_jobs_waiting_stoppers(self):
        jobs_to_retry, self.jobs_to_retry = self.jobs_to_retry, {}
        for job_id, deadline in jobs_to_retry:
            self._register_stop_waiting(job_id, deadline)

    def update_tag_deps(self, tagStorage):
        with self.lock:
            if isinstance(self.done_tag, TagBase):
                self.done_tag = tagStorage.AcquireTag(self.done_tag.name)
            for jid in self.job_done_tag:
                if isinstance(self.job_done_tag[jid], TagBase):
                    self.job_done_tag[jid] = tagStorage.AcquireTag(self.job_done_tag[jid].name)

            self.wait_dep_tags = tagset(self.wait_dep_tags)
            for tag in map(tagStorage.AcquireTag, self.wait_dep_tags):
                if tag.IsLocallySet():
                    self._process_tag_set_event(tag)

    def _release_links(self):
        tmpLinks, self.bin_links = self.bin_links, {}
        while tmpLinks:
            binname, file = tmpLinks.popitem()
            if isinstance(file, BinaryFile):
                # 'try' was a workaround for race-conditions in self. Now j.i.c
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
                self.bin_links[binname] = filehash

    def _create_link(self, binname, file):
        file.Link(self, binname)
        self.bin_links[binname] = file

    def _add_link(self, binname, file):
        if binname in self.bin_links:
            old_file = self.bin_links.pop(binname)
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
        tmpLinks, self.bin_links = self.bin_links, {}
        while tmpLinks:
            binname, link = tmpLinks.popitem()
            file = self._vivify_link(context, link)
            if file is not None:
                self._create_link(binname, file)

    def _are_links_alive(self, context):
        return all(self._vivify_link(context, link) for link in self.bin_links.itervalues())

    def _release_place(self):
        with self.lock:
# TODO XXX TODO
# TODO XXX TODO sandbox, active_jobs_cache instead of _active_jobs
# TODO XXX TODO
            if self._has_active_jobs(): # FIXME Like an assert
                logging.error("_release_place of %s in %s with active jobs" % (self.id, self._repr_state))
            self._kill_jobs_drop_results() # TODO INDEFINITE_TIME

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

        while not self.id:
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
        if runner.returncode_robust == 0 and not runner.is_cancelled():
            self.succeed_jobs.add(job.id)

            if self.job_done_tag.get(job.id):
                self.job_done_tag[job.id].Set()

            for nid in self.jobs_graph[job.id]:
                self.wait_job_deps[nid].remove(job.id)
                if not self.wait_job_deps[nid]:
                    self.jobs_to_run.add(nid)

        elif job.tries < job.maxTryCount:
            delay = job.retry_delay or job.ERR_PENALTY_FACTOR ** job.tries
            self._register_stop_waiting(job.id, time.time() + delay)

        else:
            self.failed_jobs.add(job.id)

            # Чтобы не вводить в заблуждение GetJobToRun
            self._update_repr_state()

            if self.kill_all_jobs_on_error:
                self._kill_jobs_drop_results()

    # TODO Check conditions again
        if len(self.succeed_jobs) == len(self.jobs):
            self._change_state(ImplState.SUCCESSFULL)
        # we can't start new jobs on _kill_jobs_drop_results=False
        elif not self.active_jobs_cache and self.failed_jobs:
            self._change_state(ImplState.ERROR)
        else:
            self._update_repr_state()

    def _register_stop_waiting(self, job_id, deadline):
        stop_id = None
        stop_waiting = lambda : self._stop_waiting(stop_id)
        stop_id = id(stop_waiting)

        cancel = delayed_executor.add(stop_waiting, deadline)

        self.jobs_to_retry[stop_id] = (job_id, cancel, deadline)

    def _cancel_all_wait_stoppers(self):
        with self.lock:
            for job_id, cancel, deadline in self.jobs_to_retry.values():
                cancel()
                self.jobs_to_run.add(job_id)

            self.jobs_to_retry.clear()

    def _stop_waiting(self, stop_id):
        with self.lock:
            descr = self.jobs_to_retry.pop(stop_id, None)

            if descrs None: # cancelled
                return

            job_id = descr[0]

            had_to_run = bool(self.jobs_to_run)
            self.jobs_to_run.add(job_id)

            if not had_to_run:
                self._update_repr_state()

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

    def _can_change_state(self, dst):
        src = self._impl_state

        if src == ImplState.ACTIVE and dst == ImplState.HISTORIED:
            return self._is_SUSPENDED()

        return dst in ImplState.allowed[src]

    def _change_state(self, state):
        with self.lock:
            if state == self._impl_state:
                #logging.warning("packet %s useless state change to current %s" % (self.id, state))
                return

            assert self._can_change_state(state), \
                "packet %s\tincorrect state change request %r => %r" % (self.name, self._impl_state, state)

            if state == ImplState.ACTIVE and not self.jobs:
                state = ImplState.SUCCESSFULL

            self._impl_state = state
            self._update_repr_state()

            if state in [ImplState.ERROR, ImplState.BROKEN]:
                self._send_email_on_error_state()

            if state == ImplState.SUCCESSFULL:
                if self.done_tag:
                    self.done_tag.Set()

            if state in [ImplState.SUCCESSFULL, ImplState.HISTORIED, ImplState.BROKEN]:
                self._release_place() # will kill jobs if need

    def _update_repr_state(self):
        new = self._calc_repr_state()

        if new == self._repr_state:
            return

        self._repr_state = new
        self.history.append((new, time.time()))
        logging.debug("packet %s\tnew state %r", self.name, new)

        # TODO logging.debug("packet %s\twaiting for %s sec", self.name, delay)

        self.FireEvent("change") # queue.relocatePacket

    def _is_SUSPENDED(self): # TODO
        return self._impl_state == ImplState.ACTIVE \
            and self.dont_run_new_jobs and not self.active_jobs_cache

    def rpc_remove(self):
        with self.lock:
            new_state = ImplState.HISTORIED
            if self._impl_state == new_state:
                return
            if not self._can_change_state(new_state):
                raise RpcUserError(RuntimeError("Can't remove packet in %s state" % self._repr_state))
            self._change_state(new_state)

    def RemoveAsOld(self):
        with self.lock:
            self._change_state(ImplState.HISTORIED)

    def _mark_as_failed_on_recovery(self):
        with self.lock:
            if self._impl_state == ImplState.BROKEN:
                return
            self._change_state(ImplState.BROKEN)

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
        if self._impl_state in [ImplState.BROKEN, ImplState.HISTORIED]:
            if self.directory:
                self._release_place()

        # Can exists only in tempStorage or in backups wo locks+backup_in_child
        elif self._impl_state == ImplState.UNINITIALIZED:
            raise RuntimeError("Can't restore packets in UNINITIALIZED state")
# TODO TODO
# TODO TODO
# TODO TODO
# TODO TODO
        #elif self._impl_state == PacketState.NONINITIALIZED: # XXX FIXME TODO
            #self._recover_noninitialized(ctx) # XXX FIXME TODO

        if self.directory:
            self._try_recover_directory(ctx)
        else:
            directory = os.path.join(ctx.packets_directory, self.id)
            if os.path.isdir(directory):
                shutil.rmtree(directory, onerror=None)

    # FIXME
        self.failed_jobs.clear() # TODO Better
        self.active_jobs_cache.clear() # TODO Better
        self._init_job_deps_graph()

        self._update_repr_state()

    def try_recover_after_backup_loading(self, ctx):
        descr = '[%s, directory = %s]' % (self, self.directory)

        try:
            self._try_recover_after_backup_loading(ctx)
        except Exception as e:
            logging.exception("Failed to recover packet %s" % descr)
            self._mark_as_failed_on_recovery()

    #def _recover_noninitialized(self, ctx): # XXX FIXME TODO
        #dir = None
        #if self.directory:
            #dir = self.directory
        #elif self.id:
            #dir = os.path.join(ctx.packets_directory, self.id)
        #else:
            #raise RuntimeError("No .id in NONINITIALIZED packet %s" % self.id)

        #self._release_place()
        #self._create_place_if_need(ctx)

    def _notify_incorrect_action(self):
        def make(ctx):
            if not(ctx.send_emails and ctx.send_emergency_emails):
                return
            return messages.FormatPacketEmergencyError(ctx, self)

        self._send_email(make)

    def _send_email_on_error_state(self):
        def make(ctx):
            if not ctx.send_emails:
                return
            if not ctx.send_emergency_emails and self._impl_state == ImplState.BROKEN:
                return
            return messages.FormatPacketErrorStateMessage(ctx, self)

        self._send_email(make)

    def _send_reset_notification(self, tag_name, comment, will_reset):
        def make(ctx):
            return messages.FormatPacketResetNotificationMessage(
                ctx=ctx, pck=self, comment=comment, tag_name=tag_name,
                will_reset=will_reset)

        self._send_email(make)

    def send_job_long_execution_notification(self, job):
        def make(ctx):
            with self.lock:
                return messages.FormatLongExecutionWarning(ctx, job)

        self._send_email(make)

    def _send_email(self, make):
        if not self.notify_emails:
            return

        ctx = self._get_scheduler_ctx()

        try:
            msg = make(ctx)
        except:
            logging.exception("Failed to format email for %s" % self.id)
        else:
            if msg:
                ctx.send_email_async(self.notify_emails, msg)

    def OnUndone(self, ref):
        pass

    # Called in queue under packet lock
    def _get_working_jobs(self):
        return [self.jobs[jid] for jid in list(self.active_jobs_cache)]

    def on_job_done(self, runner):
        self._active_jobs.on_done(runner)

        with self.lock:
            self._apply_jobs_results()

    def _process_jobs_results(self, processor):
        active = self._active_jobs

        while active.has_results():
            runner = active.pop_result()
            try:
                self._process_job_result(processor, runner)
            except Exception:
                logging.exception("Failed to process job result for %s" % runner.job)

    def _process_job_result(self, processor, runner):
        job = runner.job
        assert job.id in self.jobs

        logging.debug("job %s\tdone [%s]", job.shell, job.last_result())

        self.active_jobs_cache.discard(job.id)
        self.FireEvent("job_done", job) # queue.working.discard(job)

        if processor:
            processor(job, runner)

    def _apply_jobs_results(self):
        self._process_jobs_results(self._apply_job_result)

    def _drop_jobs_results(self):
        def revert_leaf(job, runner):
            self.jobs_to_run.add(job.id)
        self._process_jobs_results(revert_leaf)

    def _kill_jobs(self):
        if self._can_run_jobs_right_now():
            logging.error("_kill_jobs on %s in %s" % (self.id, self._repr_state)) # TODO better message

        active = self._active_jobs

        active.terminate()
        active.wait_running_empty()

        self._close_streams()

    def _kill_jobs_drop_results(self):
        self._kill_jobs()
        self._drop_jobs_results()

    def OnDone(self, ref):
        if isinstance(ref, TagBase):
            with self.lock:
                if self._impl_state != ImplState.HISTORIED: # FIXME
                    self._process_tag_set_event(ref)

    def rpc_add_job(self, shell, parents, pipe_parents, set_tag, tries,
            max_err_len, retry_delay, pipe_fail, description, notify_timeout,
            max_working_time, output_to_status):

        with self.lock:
            if self._impl_state != ImplState.UNINITIALIZED:
                raise RpcUserError(RuntimeError("incorrect state for \"Add\" operation: %s" % self._impl_state))

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
                self.job_done_tag[job.id] = set_tag

            return job

    def _add_job_to_graph(self, job):
        # wait_job_deps[child] -> parents
        # jobs_graph[parent]   -> children (constant between calls to Add)

        parents = job.parents

        self.jobs_graph[job.id] = []
        for p in parents:
            self.jobs_graph[p].append(job.id)

        self.wait_job_deps[job.id] = [jid for jid in parents if jid not in self.succeed_jobs]

        if not self.wait_job_deps[job.id]:
            self.jobs_to_run.add(job.id)

    # Modify:   wait_job_deps, jobs_to_run
    # Consider: jobs_to_retry, succeed_jobs
    def _init_job_deps_graph(self):
        def visit(startJID):
            st = [[startJID, 0]]
            discovered.add(startJID)

            while st:
                # num - number of neighbours discovered
                jid, num = st[-1]
                adj = self.jobs_graph[jid]
                if num < len(adj):
                    st[-1][1] += 1
                    nid = adj[num]
                    self.wait_job_deps[nid].add(jid)
                    if nid not in discovered:
                        discovered.add(nid)
                        st.append([nid, 0])
                    elif nid not in finished:
                        raise RuntimeError("job dependencies cycle exists")
                else:
                    st.pop()
                    finished.add(jid)

        with self.lock:
            assert not self._active_jobs and not self.active_jobs_cache, "Has active jobs"
            assert not self.failed_jobs, "Has failed jobs"

            discovered = set()
            finished = set()

            self.wait_job_deps = {jid: set() for jid in self.jobs if jid not in self.succeed_jobs}

            for jid in self.jobs:
                if jid not in discovered and jid not in self.succeed_jobs:
                    visit(jid)

            jobs_to_retry = set(descr[0] for descr in self.jobs_to_retry.values())

            self.jobs_to_run = set(
                jid for jid in discovered
                    if not self.wait_job_deps[jid] and jid not in jobs_to_retry
            )

    # FIXME Diese Funktion ist Wunderwaffe
    def _resume(self, resume_workable=False, silent_noop=False):
        raise AssertionError("FUCK YOU BITCH!")

    def History(self):
        return self.history or []

    # FIXME It's better for debug to allow this call from RPC without lock
    #       * From messages it's called under lock actually
    def Status(self):
        return self._status()

    @property
    def waitingDeadline(self):
        # TODO Using self.jobs_to_retry
        return 1

    def _status(self):
        history = self.History()
        total_time = history[-1][1] - history[0][1]
        wait_time = 0

        for ((state, start_time), (_, end_time)) in zip(history, history[1:] + [("", time.time())]):
            if state in (PacketState.SUSPENDED, PacketState.WAITING):
                wait_time += end_time - start_time

        result_tag = self.done_tag.name if self.done_tag else None

        waiting_time = max(int(self.waitingDeadline - time.time()), 0) \
            if self._repr_state == PacketState.WAITING else None

        all_tags = list(self.all_dep_tags)

        status = dict(name=self.name,
                      state=self._repr_state,
                      wait=list(self.wait_dep_tags),
                      all_tags=all_tags,
                      result_tag=result_tag,
                      priority=self.priority,
                      notify_emails=self.notify_emails,
                      history=history,
                      total_time=total_time,
                      wait_time=wait_time,
                      last_modified=history[-1][1],
                      waiting_time=waiting_time)
        extra_flags = set()
        if self._impl_state == ImplState.BROKEN:
            extra_flags.add("can't-be-recovered")
        if self.dont_run_new_jobs:
            extra_flags.add("manually-suspended")
        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

    # XXX XXX WTF XXX XXX
        # FIXME WHY? no: HISTORIED, NONINITIALIZED, CREATED
        #if self._impl_state not in [ImplState.HISTORIED, ImplState.UNINITIALIZED]:
        if self._repr_state in (PacketState.ERROR, PacketState.SUSPENDED,
                          PacketState.WORKABLE, PacketState.PENDING,
                          PacketState.SUCCESSFULL, PacketState.WAITING):
            status["jobs"] = []
            for jid, job in self.jobs.iteritems():
                result = job.last_result()
                results = []
                if result:
                    results = [safeStringEncode(str(res)) for res in job.results]

                state = "done" if jid in self.succeed_jobs \
                    else "working" if jid in self.active_jobs_cache \
                    else "pending" if jid in self.jobs_to_run \
                    else "errored" if result and not result.IsSuccessfull() \
                    else "suspended"

                wait_jobs = []
                #if self.active_jobs_cache:
                if self._repr_state == PacketState.WORKABLE:
                    wait_jobs = map(str, self.wait_job_deps.get(jid, []))

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

    # TODO Don't support this in ACTIVE for sandbox
    def rpc_add_binary(self, binname, file):
        with self.lock:
            self._add_link(binname, file)

    def rpc_suspend(self, kill_jobs=False):
        with self.lock:
            if self._impl_state == ImplState.BROKEN: # legacy check
                raise RpcUserError(
                    RuntimeError(
                        "Can't suspend ERROR'ed packet %s with RCVR_ERROR flag set" % self.id))

            self.dont_run_new_jobs = True
            self._update_repr_state() # maybe from PENDING to WORKABLE

            if kill_jobs:
                # FIXME In ideal world it's better to "apply" jobs that will be
                # finished racy just before kill(2)
                self._kill_jobs_drop_results()

            self._update_repr_state() # maybe from PENDING to WORKABLE

    def rpc_resume(self):
        with self.lock:
            if self.dont_run_new_jobs:
                self.dont_run_new_jobs = False

                if self._impl_state == ImplState.ACTIVE:
                    # Repeat legacy bullshit behaviour # FIXME DONT DO THIS ANYMORE?
                    self.jobs_to_run.update(self.failed_jobs)
                    self.failed_jobs.clear()
                    for job in self.jobs.values():
                        job.tries = 0

                    self._update_repr_state()

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

    # jobs
    # jobs_graph        = edges
    # wait_job_deps     = waitJobs
    # jobs_to_run       = leafs
    # active_jobs_cache = as_in_queue_working
    # succeed_jobs      = done
    # + jobs_to_retry
    # + failed_jobs
    #
    def _reset_jobs(self):
        self._kill_jobs_drop_results()
        assert not self.active_jobs_cache, "Has active jobs"

        for job in self.jobs.values():
            job.results = []
            job.tries = 0

        self.succeed_jobs.clear()
        self.failed_jobs.clear()
        self.jobs_to_retry.clear()

        self._init_job_deps_graph()

        self._clean_state = True # FIXME

    def _reset(self, tag_op):
        def update_tags():
            for tag, is_done in self._get_all_done_tags():
                tag_op(tag, is_done)

        # XXX force call to _change_state for packets without jobs
        #if self._clean_state and self.jobs:
            #update_tags()
            #return

        # if not(self.jobs) we will first reset, then set (by _create_place_if_need)

        # TODO Test that job_done_tag are reset
        update_tags()

        if not self._clean_state:
            self._reset_jobs()

        if not self._try_create_place_if_need():
            self._change_state(ImplState.BROKEN)
            return

        self._change_state(ImplState.ACTIVE)

    def _try_create_place_if_need(self):
        try:
            self._create_place_if_need(self._get_scheduler_ctx())
            return True
        except Exception: # TODO
            try:
                logging.exception("Failed to create place")
            except:
                pass
            return False

    def rpc_reset(self, suspend=False):
        with self.lock:
            self.dont_run_new_jobs = suspend
            self._reset(lambda tag, _: tag.Unset())

    def _get_scheduler_ctx(self):
        return PacketCustomLogic.SchedCtx
        #return self._get_scheduler().context

    def _get_all_done_tags(self):
        if self.done_tag:
            yield self.done_tag, self._impl_state == ImplState.SUCCESSFULL

        for job_id, tag in self.job_done_tag.iteritems():
            yield tag, job_id in self.succeed_jobs

    def OnReset(self, (ref, comment)):
        if isinstance(ref, TagBase):
            self._on_tag_reset(ref, comment)

    def _on_tag_reset(self, ref, comment):
        with self.lock:
            if self._impl_state in [ImplState.HISTORIED, ImplState.BROKEN]:
                return

            # FIXME Don't even update .wait_dep_tags if not self.is_resetable
            tag_name = ref.GetFullname()
            self.wait_dep_tags.add(tag_name)

            if self._impl_state == ImplState.UNINITIALIZED:
                return

            def notify(will_reset):
                self._send_reset_notification(tag_name, comment, will_reset)

            if not self.is_resetable:
                if self.notify_on_skipped_reset:
                    notify(False)
                return

            if self.notify_on_reset:
                notify(True)

            # TODO "is_done and tag.Reset(comment)" is not kosher!
            # We need reset-if-need logic in cloud_tags_server proxy
            # and "last_set_version" in /map table (not in journal):
            #
            # StartTransaction();
            # tag = ReadTag("/map");
            # if (tag.LastSetVersion > tag.LastResetVersion) {
            #   WriteTagReset("/map", "/journal");
            #   CommitTransaction();
            # } else {
            #   DropTransaction();
            # }
            #
            # FIXME This logic also may be added to local tags

            # Reset or Unset emulates legacy behaviour
            self._reset(lambda tag, is_done: tag.Reset(comment) if is_done else tag.Unset())

    def _move_to_queue(self, src_queue, dst_queue, from_rpc=False):
        with self.lock:
            if self.active_jobs_cache:
                raise as_rpc_user_error(
                    from_rpc, RuntimeError("Can't move packets with running jobs"))

            if src_queue:
                src_queue._detach_packet(self)

            if dst_queue:
                dst_queue._attach_packet(self)

    def rpc_move_to_queue(self, src_queue, dst_queue):
        self._move_to_queue(src_queue, dst_queue, from_rpc=True)

    def _attach_to_queue(self, queue):
        with self.lock:
            assert self._impl_state == ImplState.UNINITIALIZED
            self._move_to_queue(None, queue)
            self._change_state(ImplState.ACTIVE)


class LocalPacket(PacketBase):
    INCORRECT = -1

    def _has_active_jobs(self):
        return bool(self._active_jobs)

    def _can_run_jobs_right_now(self):
        # XXX TODO XXX Too complicated
        return self._impl_state == ImplState.ACTIVE and self.jobs_to_run \
                    and not (self.dont_run_new_jobs or self.failed_jobs)

    def GetJobToRun(self):
        with self.lock:
            if not self._can_run_jobs_right_now():
                return self.INCORRECT

            def get_job():
                for jid in self.jobs_to_run:
                    job = self.jobs[jid]
                    self.FireEvent("job_get", job) # queue.working.add(job)
                    self.active_jobs_cache.add(jid)
                    self.jobs_to_run.remove(jid)
                    return job

            job = get_job()
            self._clean_state = False

            self._update_repr_state()

            runner = JobRunner(job)
            self._active_jobs.add(runner)
            return runner


JobPacket = LocalPacket
# В этот раз можно за счёт разного названия класса десериализовать и конвертнуть
# XXX XXX FIXME JobPacket = class(Unpickable и тут все старые поля
# XXX XXX FIXME А потом format_version и конвертация


class SandboxPacket(PacketBase):
    pass
