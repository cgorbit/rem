# -*- coding: utf-8 -*-
from __future__ import with_statement
import tempfile
import os
import time
import shutil
import errno
import base64
import cPickle

from callbacks import CallbackHolder, ICallbackAcceptor, TagBase, tagset
from common import BinaryFile, PickableRLock, Unpickable, safeStringEncode, as_rpc_user_error, RpcUserError
from job import Job, JobRunner
import osspec
import messages
from rem_logging import logger as logging
import job_graph
from packet_common import ReprState
import sandbox_packet

class ImplState(object):
    UNINITIALIZED = '_UNINITIALIZED' # jobs, files and queue not set
    WAIT_TAGS     = '_WAIT_TAGS'
    WAIT_PREV_EXECUTOR = '_WAIT_PREV_EXECUTOR'
    PENDING       = '_PENDING'
    RUNNING       = '_RUNNING'
    SUCCESSFULL   = '_SUCCESSFULL'
    ERROR         = '_ERROR'
    BROKEN        = '_BROKEN'
    DESTROYING    = '_DESTROYING'
    HISTORIED     = '_HISTORIED'

    allowed = {
        UNINITIALIZED: [BROKEN, WAIT_TAGS, PENDING, DESTROYING, HISTORIED],
        WAIT_TAGS:     [BROKEN, PENDING, DESTROYING, HISTORIED], # + SUCCESSFULL
        WAIT_PREV_EXECUTOR # if .files_was_modified (resources)
        PENDING:       [BROKEN, RUNNING], # + DESTROYING, HISTORIED
        RUNNING:       [BROKEN, WAIT_TAGS, SUCCESSFULL, ERROR], # + DESTROYING, HISTORIED (see _can_change_state)
        SUCCESSFULL:   [BROKEN, PENDING, DESTROYING, HISTORIED, WAIT_TAGS],
        ERROR:         [BROKEN, PENDING, DESTROYING, HISTORIED, WAIT_TAGS],
        BROKEN:        [DESTROYING, HISTORIED],
        DESTROYING:    [HISTORIED],
        HISTORIED:     [],
    }

#################################################################################
# stopping
#   1. позволяет не звать больше одного раза .stop(), если `g' почему-то так не умеет
#   2. позволяет правильно работать даже тогда, когда `g' использует свой lock,
#      а не общий с self (иначе race-condition)
#
# need_to_start просто нужен (если, конечно, почему-то это не реализовано в `g')
#
# XXX Оказывается я это уже напрограммировал в SandboxJobGraphExecutorProxy
def something_with_restart():
    with self.lock:
        if self.stopping:
            self.need_to_start = True
        else:
            if self.g.is_null():
                self.g.start()
            else:
                self.g.stop()
                self.stopping = True
                self.need_to_start = True

def something_with_stop():
    with self.lock:
        if not self.stopping:
            self.g.stop()
            self.stopping = True
        self.need_to_start = False

def on_g_stop(self):
    with self.lock:
        assert self.stopping
        self.stopping = False

        if self.need_to_start:
            self.need_to_start = False
            self.g.start()
#################################################################################

is_inited = bool(.queue)
len(.jobs)
len(.wait_dep_tags)
type(.running) == bool
type(.is_broken) == bool
type(.finish_status) == (False, True, None)
type(.destroying) == bool
return _graph_executor.is_suspended() or _graph_executor.is_null()
return self._graph_executor.is_null()

type(.files_was_modified) == bool

HISTORIED: lambda : self._release_place_if_need()
RUNNING:   lambda : if self._graph_executor.is_null(): self._graph_executor.start()

def _on_job_graph_become_empty(self):
    if self.waiting_prev_executor:
        self.waiting_prev_executor = False
    self._update_state()

def on_tag_set(self):
    ...
    if not self.wait_dep_tags:
        self._update_state()

def destroy(self):
    

# TODO TEST
def _calc_state(self):
    is_inited = bool(self.queue)

    graph = self._graph_executor

    if self.destroying:
        if self._graph_executor.is_null():
            return HISTORIED
        else:
            return DESTROYING

    elif self.is_broken:
        return BROKEN

    elif not is_inited:
        return UNINITIALIZED

    elif self.wait_dep_tags:
        return WAIT_TAGS

    elif not self.jobs:
        return SUCCESSFULL

    elif self._execution_finished_sucessfully is not None:
        return SUCCESSFULL \
            if self._execution_finished_sucessfully \
            else ERROR

    elif not graph.does_allow_to_run_jobs():
        if graph.has_running_jobs():
            return PAUSING
        else:
                          # XXX
            return PAUSED # Здесь можно обновлять файлы/ресурсы, но после этого
                          # мы должны быть уверены, что Граф будет stop/start'ed,
                          # например, за счёт .files_was_modified

    elif not graph.is_null():
        if self.waiting_prev_executor:
            return WAIT_PREV_EXECUTOR
        else:
            return RUNNING

    else:
        return PENDING

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


class NotAllowedStateChangeError(AssertionError):
    pass


class PacketCustomLogic(object):
    SchedCtx = None

    @classmethod
    def UpdateContext(cls, context):
        cls.SchedCtx = context


# JobGraph: wait_job_deps=dict,
#           succeed_jobs=set,
#           failed_jobs=set,
#           jobs_to_run=set,
#           jobs_to_retry=dict,
#           active_jobs_cache=always(set),
#           created_inputs=set,
#           dont_run_new_jobs=bool,
#
# Packet: repr_state
#         history
#
# Job: tries
#      results
#      cached_working_time


class JobGraph(object):
    def __init__(self, jobs, kill_all_jobs_on_error):
        self.jobs = jobs
        self.kill_all_jobs_on_error = kill_all_jobs_on_error

    def build_deps_dict():
        graph = {}

        for job in self.jobs.values:
            graph[job.id] = []
            for parent_id in job.parents:
                graph[parent_id].append(job.id)

        return graph


def reraise(msg):
    t, e, tb = sys.exc_info()
    raise RuntimeError, RuntimeError('%s: %s' % (msg, e)), tb


def always(ctor):
    def always(*args):
        return ctor()
    return always


class PacketBase(Unpickable(
                           lock=PickableRLock,

                           jobs=dict,
                           #jobs_graph=dict,

                           all_dep_tags=set,
                           wait_dep_tags=set,

                           job_done_tag=dict,
                           #done_tag=object,

                           bin_links=dict,
                           sbx_files=dict,
                           directory=lambda *args: args[0] if args else None,

                           state=(str, ReprState.ERROR),
                           _impl_state=(str, ImplState.BROKEN),

                           notify_emails=list,
                           is_resetable=(bool, True),
                           notify_on_reset=(bool, False),
                           notify_on_skipped_reset=(bool, True),

                           history=list,
                          ),
                CallbackHolder,
                ICallbackAcceptor):

    def __init__(self, name, priority, context, notify_emails, wait_tags=(),
                 set_tag=None, kill_all_jobs_on_error=True, is_resetable=True,
                 notify_on_reset=False, notify_on_skipped_reset=True):
        super(PacketBase, self).__init__()
        self.name = name
        self.id = None
        self.directory = None
        self.queue = None

        self._create_place_if_need(context)
        self.id = os.path.split(self.directory)[-1]

        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.is_resetable = is_resetable
        self.done_tag = set_tag

        self._set_waiting_tags(wait_tags)

        self._graph_executor = AlmostDummyGraphExecutorThatRemebersDontRunNewJobsFlag()

        self._impl_state = ImplState.UNINITIALIZED
        self._update_repr_state()

    def _calc_repr_state(self):
        state = self._impl_state

        if state == ImplState.UNINITIALIZED:
            return ReprState.CREATED

        elif state in [ImplState.ERROR, ImplState.BROKEN]:
            return ReprState.ERROR

        elif state in ImplState.HISTORIED:
            return ReprState.HISTORIED

        elif state in ImplState.PENDING:
            return ReprState.PENDING

        elif state == ImplState.SUCCESSFULL:
            return ReprState.SUCCESSFULL

        elif state == ImplState.WAIT_TAGS:
            return ReprState.SUSPENDED

        elif state == ImplState.DESTROYING and self._graph_executor.is_null():
            return ReprState.HISTORIED

        assert state in [ImplState.ACTIVE, ImplState.DESTROYING]

        # XXX PacketFlag.USER_SUSPEND -> means -> .dont_run_new_jobs
        # XXX PacketFlag.RCVR_ERROR   -> means -> ._impl_state == BROKEN

        return self._graph_executor.get_repr_state()

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_tag']:
            sdict['done_tag'] = sdict['done_tag'].name

        job_done_tag = sdict['job_done_tag'] = sdict['job_done_tag'].copy()
        for job_id, tag in job_done_tag.iteritems():
            job_done_tag[job_id] = tag.name

        return sdict

    def _create_place_if_need(self, context):
        #logging.info("packet init: %r %s", self, self.state) # FIXME
        if self.directory is None:
            self._create_place(context)

    def __repr__(self):
        return "<%s(id: %s; name: %s; state: %s)>" % (type(self).__name__, self.id, self.name, self.state)

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
                if self._impl_state == ImplState.WAIT_TAGS: # if self.queue
                    self._become_pending()
                else:
                    self._update_repr_state()

    def vivify_done_tags_if_need(self, tagStorage):
        with self.lock:
            if isinstance(self.done_tag, str):
                self.done_tag = tagStorage.AcquireTag(self.done_tag)
            for jid, cur_val in self.job_done_tag.iteritems():
                if isinstance(cur_val, str):
                    self.job_done_tag[jid] = tagStorage.AcquireTag(cur_val)

    def vivify_jobs_waiting_stoppers(self):
        pass

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
        if self.directory:
            self._create_link(binname, file)
        else:
            self.bin_links[binname] = file.checksum

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
            self._release_links()

            if self.directory and os.path.isdir(self.directory):
                try:
                    shutil.rmtree(self.directory, onerror=None)
                except Exception, e:
                    logging.exception("Packet %s release place error", self.id)

            self.directory = None

    def _create_place(self, context):
        assert not self._graph_executor or self._graph_executor.is_null() # FIXME

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

    def _can_change_state(self, dst):
        src = self._impl_state

        if src in [ImplState.PENDING, ImplState.ACTIVE] and dst == ImplState.DESTROYING:
            return self._graph_executor.is_suspended() or self._graph_executor.is_null()

        elif src in [ImplState.PENDING, ImplState.ACTIVE] and dst == ImplState.HISTORIED:
            return self._graph_executor.is_null()

        elif src == ImplState.UNINITIALIZED and dst == ImplState.SUCCESSFULL:
            return not self.jobs and not self.wait_dep_tags

        elif src == ImplState.WAIT_TAGS and dst == ImplState.SUCCESSFULL:
            return not self.jobs

        return dst in ImplState.allowed[src]

    def _become_pending_or_wait_tags(self):
        if self.wait_dep_tags:
            self._change_state(ImplState.WAIT_TAGS)
        else:
            self._become_pending()

    def _become_pending(self):
        assert not self.wait_dep_tags
        assert self._impl_state != ImplState.ACTIVE

        if not self.jobs: # FIXME and not self._graph_executor.dont_run_new_jobs:
            state = ImplState.SUCCESSFULL
        else:
            state = ImplState.ACTIVE

        self._change_state(state)

    def _change_state(self, state):
        with self.lock:
            #if state == self._impl_state:
                #logging.warning("packet %s useless state change to current %s" % (self.id, state))
                #return

            if not self._can_change_state(state):
                raise NotAllowedStateChangeError(
                    "packet %s\tincorrect state change request %r => %r" \
                        % (self.name, self._impl_state, state))

            self._impl_state = state
            self._update_repr_state()

            if state in [ImplState.ERROR, ImplState.BROKEN]:
                self._send_email_on_error_state()

            elif state == ImplState.RUNNING:
                if self._graph_executor.is_null():
                    self._graph_executor.start()

            elif state == ImplState.SUCCESSFULL:
                if self.done_tag:
                    self.done_tag.Set()

            if state in [ImplState.SUCCESSFULL, ImplState.HISTORIED, ImplState.BROKEN]:
                self._release_place() # will kill jobs if need

    def _update_repr_state(self):
        new = self._calc_repr_state()

        if new == self.state:
            return

        self.state = new
        self.history.append((new, time.time()))
        logging.debug("packet %s\tnew state %r", self.name, new)

        # TODO logging.debug("packet %s\twaiting for %s sec", self.name, delay)

        self.queue._on_packet_state_change(self)
        self.FireEvent("change") # PacketNamesStorage

    def rpc_remove(self):
        with self.lock:
            try:
                self.destroy()
            except NotAllowedStateChangeError:
                raise RpcUserError(RuntimeError("Can't remove packet in %s state" % self.state))

    def destroy(self):
        with self.lock:
            if self.destroying:
                return

            if not self._can_change_state(new_state):
                raise NonDestroyingStateError()

            self._graph_executor.reset()
            self._update_state()

    def _on_job_graph_become_empty(self):
        with self.lock:
            self._update_state()

    def _mark_as_failed_on_recovery(self):
        with self.lock:
            if self.is_broken:
                return
            self.is_broken = True
            self._update_state()

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
        #elif self._impl_state == ReprState.NONINITIALIZED: # XXX FIXME TODO
            #self._recover_noninitialized(ctx) # XXX FIXME TODO

        if self.directory:
            self._try_recover_directory(ctx)
        else:
            directory = os.path.join(ctx.packets_directory, self.id)
            if os.path.isdir(directory):
                shutil.rmtree(directory, onerror=None)

        self._graph_executor.recover_after_backup_loading()

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

            parents = list(set(parents + pipe_parents))
            pipe_parents = list(set(pipe_parents))

            for dep_id in parents:
                if dep_id not in self.jobs:
                    raise RpcUserError(RuntimeError("No job with id = %s in packet %s" % (dep_id, self.pck_id)))

            job = Job(shell, parents, pipe_parents, self.id, max_try_count=tries,
                      max_err_len=max_err_len, retry_delay=retry_delay,
                      pipe_fail=pipe_fail, description=description, notify_timeout=notify_timeout, max_working_time=max_working_time, output_to_status=output_to_status)

        ###
            self.jobs[job.id] = job


            #self.jobs_graph[job.id] = []
            #for p in job.parents:
                #self.jobs_graph[p].append(job.id)
        ###

            if set_tag:
                self.job_done_tag[job.id] = set_tag

            return job

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
            if state in (ReprState.SUSPENDED, ReprState.WAITING):
                wait_time += end_time - start_time

        result_tag = self.done_tag.name if self.done_tag else None

        waiting_time = max(int(self.waitingDeadline - time.time()), 0) \
            if self.state == ReprState.WAITING else None

        all_tags = list(self.all_dep_tags)

        status = dict(name=self.name,
                      state=self.state,
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

        if self.state == ReprState.SUSPENDED and self._impl_state == ImplState.ACTIVE:
            extra_flags.add("manually-suspended")

        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

    # XXX XXX WTF XXX XXX
        # FIXME WHY? no: HISTORIED, NONINITIALIZED, CREATED
        #if self._impl_state not in [ImplState.HISTORIED, ImplState.UNINITIALIZED]:
        #if self.state in (ReprState.ERROR, ReprState.SUSPENDED,
                          #ReprState.WORKABLE, ReprState.PENDING,
                          #ReprState.SUCCESSFULL, ReprState.WAITING):
        if self._graph_executor:
            status["jobs"] = self._graph_executor.produce_detailed_status()

        return status

    def rpc_add_binary(self, binname, file):
        with self.lock:
            self.files_was_modified = True
            self._add_link(binname, file)

    def rpc_suspend(self, kill_jobs=False):
        with self.lock:
            if self._impl_state == ImplState.BROKEN: # legacy check
                raise RpcUserError(
                    RuntimeError(
                        "Can't suspend ERROR'ed packet %s with RCVR_ERROR flag set" % self.id))

            self._graph_executor.disallow_to_run_jobs(kill_running=kill_jobs)

    def _try_create_place_if_need(self):
        try:
            self._create_place_if_need(self._get_scheduler_ctx())
            return True
        except Exception:
            logging.exception("Failed to create place")
            self._change_state(ImplState.BROKEN)
            self._graph_executor.stop()
            return False

    def rpc_resume(self):
        with self.lock:
            # `reset_tries' is legacy behaviour of `rpc_resume'
            self._graph_executor.allow_to_run_jobs(reset_tries=True)

    def _update_done_tags(self, op):
        for tag, is_done in self._get_all_done_tags():
            op(tag, is_done)

    #def _reset(self, tag_op):
        # XXX force call to _change_state for packets without jobs
        #if self._clean_state and self._graph_executor.jobs:
            #update_tags()
            #return

        # if not(self._graph_executor.jobs) we will first reset, then set (by _create_place_if_need)

        # TODO Test that job_done_tag are reset
        #update_tags()

        #if self._local_directory_is_modified(): # FIXME
            #self._release_place_if_need()

        #if self.wait_dep_tags:
            #self._graph_executor.stop()

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

    def rpc_reset(self, suspend=False):
        with self.lock:
            self._update_done_tags(lambda tag, _: tag.Unset())

            if not self._try_create_place_if_need():
                return

            g = self._graph_executor

            if suspend is None:
                pass
            elif suspend:
                g.disallow_to_run_jobs()
            else:
                g.allow_to_run_jobs()

            if self.wait_dep_tags:
                g.stop()
            else:
                g.restart()

            self._update_state()

    def _on_tag_reset(self, ref, comment):
        with self.lock:
            if self.destroying:
                return

            # FIXME Don't even update .wait_dep_tags if not self.is_resetable
            tag_name = ref.GetFullname()
            self.wait_dep_tags.add(tag_name)

            if self._impl_state == ImplState.UNINITIALIZED:
            #if not self.queue:
                return

            def notify(will_reset):
                self._send_reset_notification(tag_name, comment, will_reset)

            if not self.is_resetable:
                if self.notify_on_skipped_reset:
                    notify(False)
                return

            if self.notify_on_reset:
                notify(True)

            # Reset or Unset emulates legacy behaviour
            self._reset(lambda tag, is_done: tag.Reset(comment) if is_done else tag.Unset())

            self._update_state()

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

    def _move_to_queue(self, src_queue, dst_queue, from_rpc=False):
        with self.lock:
            self._check_can_move_beetwen_queues()

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

            self._graph_executor = self._create_job_graph_executor()
            self._become_pending_or_wait_tags()

    def make_job_graph(self):
        return JobGraph(self.jobs, self.kill_all_jobs_on_error)


class _LocalPacketJobGraphOps(object):
    def __init__(self, pck):
        self.pck = pck

    def get_io_directory(self):
        return self.pck.directory

    get_working_directory = get_io_directory

    def update_repr_state(self):
        self.pck._update_repr_state()

    def stop_waiting(self, stop_id):
        self.pck._stop_waiting(stop_id)

    def del_working(self, job):
        self.queue._on_job_done(self)

    def add_working(self, job):
        self.queue._on_job_get(self)

    def set_successfull_state(self):
        self.pck._change_state(ImplState.SUCCESSFULL)

    def set_errored_state(self):
        self.pck._change_state(ImplState.ERROR)

    def job_done_successfully(self, job_id):
        tag = self.pck.job_done_tag.get(job_id)
        if tag:
            tag.Set()

    def create_job_runner(self, job):
        return JobRunner(self, job)

    def on_job_done(self, runner):
        self.pck.on_job_done(runner)

    def create_file_handles(self, job):
        return self.pck._graph_executor.create_job_file_handles(job)

    def notify_long_execution(self, job):
        logging.warning("Packet's '%s' job '%s' execution takes too long time", self.pck.name, job.id)
        self.pck.send_job_long_execution_notification(self, job)

    def start_process(self, args, kwargs):
        ctx = PacketCustomLogic.SchedCtx
        return ctx.run_job(*args, **kwargs)


class NotWorkingStateError(RuntimeError):
    pass

class NonDestroyingStateError(RuntimeError):
    pass


class LocalPacket(PacketBase):
    def _create_job_graph_executor(self):
        return job_graph.JobGraphExecutor(
            _LocalPacketJobGraphOps(self), # TODO Cyclic reference
            self.id,
            self.make_job_graph(),
        )

    def _can_run_jobs_right_now(self):
        return self._impl_state == ImplState.ACTIVE \
            and self._graph_executor.can_run_jobs_right_now()

    def get_job_to_run(self):
        with self.lock:
            if not self._can_run_jobs_right_now():
                raise NotWorkingStateError("Can't run jobs in % state" % self.state)

            runner = self._graph_executor.get_job_to_run()
            self._update_repr_state() # FIXME in JobGraphExecutor

            return runner

    def vivify_jobs_waiting_stoppers(self):
        with self.lock:
            self._graph_executor.vivify_jobs_waiting_stoppers()

    # Called in queue under packet lock
    def _get_working_jobs(self):
        return self._graph_executor.get_working_jobs()

    def on_job_done(self, runner):
        self._graph_executor.on_job_done(runner)

        with self.lock:
            self._graph_executor.apply_jobs_results()

    def _create_job_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)

    def _stop_waiting(self, stop_id):
        with self.lock:
            self._graph_executor.stop_waiting(stop_id)

    def _check_can_move_beetwen_queues(self):
        if self._graph_executor.has_running_jobs():
            raise as_rpc_user_error(
                from_rpc, RuntimeError("Can't move packets with running jobs"))


SandboxPacket = sandbox_packet.SandboxPacket
