# -*- coding: utf-8 -*-
from __future__ import with_statement
import tempfile
import os
import time
import shutil
import errno
import sys
import copy

from callbacks import CallbackHolder, ICallbackAcceptor, TagBase, tagset
from common import BinaryFile, PickableRLock, Unpickable, safeStringEncode, as_rpc_user_error, RpcUserError
from job import Job, JobRunner, _DEFAULT_STDERR_SUMMAY_LEN as DEFAULT_JOB_MAX_ERR_LEN
import osspec
import messages
from rem_logging import logger as logging
import job_graph
from job_graph import GraphState
from packet_state import PacketState
ImplState = PacketState
import sandbox_remote_packet


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

    def build_deps_dict(self):
        graph = {}

        for job in self.jobs.values():
            graph[job.id] = []
            for parent_id in job.parents:
                graph.setdefault(parent_id, []).append(job.id)

        return graph


def reraise(msg):
    t, e, tb = sys.exc_info()
    raise RuntimeError, RuntimeError('%s: %s' % (msg, e)), tb


def always(ctor):
    def always(*args):
        return ctor()
    return always


class DummyGraphExecutor(object):
    state = GraphState.PENDING_JOBS

# XXX TODO detailed_status dissapeared from SandboxPacket

    def is_null(self):
        return True

    def produce_detailed_status(self):
        return {}

    def get_working_jobs(self):
        return []

    def recover_after_backup_loading(self):
        pass

    def is_cancelling(self):
        return False

    def vivify_jobs_waiting_stoppers(self):
        pass

    def reset(self):
        pass


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

                           _repr_state=(str, ReprState.ERROR),
                           state=(str, ImplState.BROKEN),

                           notify_emails=list,
                           is_resetable=(bool, True),
                           notify_on_reset=(bool, False),
                           notify_on_skipped_reset=(bool, True),

                           history=list,

                           #stopping_graph=bool,
                           #need_to_start_graph=bool,
                           do_not_run=bool,
                           destroying=bool,
                           is_broken=bool,
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
        self.finish_status = None
        #self.time_wait_deadline = None

        self._graph_executor = DummyGraphExecutor()

        self._create_place(context)
        self.id = os.path.split(self.directory)[-1]

# FIXME
        self.files_modified = False
        self.resources_modified = False

        self.files_sharing = None
        self.shared_files_resource_id = None

        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.is_resetable = is_resetable
        self.done_tag = set_tag

        self._set_waiting_tags(wait_tags)

        #self.state = ImplState.UNINITIALIZED
        self._update_state()

    def _is_executable(self):
        return not self.is_broken and not self.destroying

    #def _is_runnable(self):
        #return self._is_executable() and not self.wait_dep_tags and self.queue \
            #and self.jobs and self.finish_status is None \
            #and self._graph_executor.is_null()

# TODO Test this code offline with mask
    def _calc_state(self):
        is_inited = bool(self.queue)

        graph = self._graph_executor

    # FIXME Better
        is_graph_remote = isinstance(self, SandboxPacket)

        if self.destroying:
            if graph.is_cancelling():
                assert graph.state & GraphState.CANCELLED
                assert graph.state & GraphState.WORKING
                return ImplState.DESTROYING
            else:
                assert not is_graph_remote or graph.is_null()
                return ImplState.HISTORIED

        elif self.is_broken:
            return ImplState.BROKEN # may be GraphState.WORKING

        elif not is_inited:
            return ImplState.UNINITIALIZED

        elif self.wait_dep_tags:
            return ImplState.TAGS_WAIT # may be GraphState.WORKING

        elif not self.jobs:
            return ImplState.SUCCESSFULL

        elif self.finish_status is not None:
            return ImplState.SUCCESSFULL if self.finish_status else ImplState.ERROR

        elif self.do_not_run:
            if graph.is_cancelling():
                assert graph.state & GraphState.WORKING
                return ImplState.PAUSING
            else:
                assert not is_graph_remote or graph.is_null()
                return ImplState.PAUSED # Здесь можно обновлять файлы/ресурсы, но после этого...
                            # мы должны быть уверены, что Граф будет stop/start'ed,
                            # например, за счёт .files_modified/.resources_modified

# FIXME
        elif not graph.is_null() or graph.state == GraphState.TIME_WAIT: # FIXME Костыль?
# FIXME
            if graph.is_cancelling():
                assert graph.state & GraphState.WORKING
                assert graph.state & GraphState.CANCELLED # FIXME | SUSPENDED
                                                          # is_stopped contains SUSPENDED and CANCELLED
                return ImplState.PREV_EXECUTOR_STOP_WAIT

            elif graph.state == GraphState.TIME_WAIT:
                return ImplState.TIME_WAIT

            else:
                assert graph.state & GraphState.WORKING
                return ImplState.RUNNING # WORKING ?| PENDING

        elif self.files_sharing:
            return ImplState.SHARING_FILES

        else:
            return ImplState.PENDING

    def _update_state(self):
        new = self._calc_state()

        if self.state != new:
            self._change_state(new)

    @staticmethod
    def _calc_repr_state(state):
        map = {
            ImplState.UNINITIALIZED:    ReprState.CREATED,
            ImplState.ERROR:            ReprState.ERROR,
            ImplState.BROKEN:           ReprState.ERROR,
            ImplState.PENDING:          ReprState.PENDING,
            ImplState.SUCCESSFULL:      ReprState.SUCCESSFULL,

            ImplState.SHARING_FILES:    ReprState.PENDING,

            ImplState.TAGS_WAIT:        ReprState.SUSPENDED,
            ImplState.PAUSED:           ReprState.SUSPENDED,
            ImplState.PAUSING:          ReprState.SUSPENDED,

            ImplState.PREV_EXECUTOR_STOP_WAIT: ReprState.WORKABLE, # XXX Must be WORKABLE to support fast_restart
            ImplState.RUNNING:          ReprState.WORKABLE,

            ImplState.TIME_WAIT:        ReprState.WAITING,

            ImplState.DESTROYING:       ReprState.WORKABLE,
            ImplState.HISTORIED:        ReprState.HISTORIED,
        }

        return map[state]

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_tag']:
            sdict['done_tag'] = sdict['done_tag'].name

        job_done_tag = sdict['job_done_tag'] = sdict['job_done_tag'].copy()
        for job_id, tag in job_done_tag.iteritems():
            job_done_tag[job_id] = tag.name

# TODO Restart sharing in vivify
        if sdict['files_sharing']:
            sdict['files_sharing'] = True
            sdict['files_modified'] = True
# TODO Restart sharing in vivify

        return sdict

    def _create_place_if_need(self):
        #logging.info("packet init: %r %s", self, self._repr_state) # FIXME
        if self.directory is None:
            self._create_place(self._get_scheduler_ctx())

    def __repr__(self):
        return "<%s(id: %s; name: %s; state: %s)>" % (type(self).__name__, self.id, self.name, self._repr_state)

    def _set_waiting_tags(self, wait_tags):
        for tag in wait_tags:
            tag.AddCallbackListener(self)
        self.all_dep_tags = set(tag.GetFullname() for tag in wait_tags)
        self.wait_dep_tags = set(tag.GetFullname() for tag in wait_tags if not tag.IsLocallySet())

    def _process_tag_set_event(self, tag):
        if not self._is_executable():
            return

        tag_name = tag.GetFullname()

        if tag_name in self.wait_dep_tags:
            self.wait_dep_tags.remove(tag_name)

            if not self.wait_dep_tags:
                self._share_files_as_resource_if_need()
                self._update_state()

    def vivify_done_tags_if_need(self, tagStorage):
        with self.lock:
            if isinstance(self.done_tag, str):
                self.done_tag = tagStorage.AcquireTag(self.done_tag)
            for jid, cur_val in self.job_done_tag.iteritems():
                if isinstance(cur_val, str):
                    self.job_done_tag[jid] = tagStorage.AcquireTag(cur_val)

    def vivify_resource_sharing(self):
        if self.files_sharing:
            raise NotImplementedError("Resource sharing vivify is not implemented")

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

# TODO XXX TODO BROKEN

    def _create_place(self, context):
        assert self._graph_executor.is_null() # FIXME

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

    #def _can_change_state(self, dst):
        #raise NotImplementedError()

        #src = self.state

        #if src in [ImplState.PENDING, ImplState.RUNNING] and dst == ImplState.DESTROYING:
            #return self._graph_executor.is_suspended() or self._graph_executor.is_null()

        #elif src in [ImplState.PENDING, ImplState.RUNNING] and dst == ImplState.HISTORIED:
            #return self._graph_executor.is_null()

        #elif src == ImplState.UNINITIALIZED and dst == ImplState.SUCCESSFULL:
            #return not self.jobs and not self.wait_dep_tags

        #elif src == ImplState.TAGS_WAIT and dst == ImplState.SUCCESSFULL:
            #return not self.jobs

        #return dst in ImplState.allowed[src]

    #def _become_pending_or_wait_tags(self):
        #if self.wait_dep_tags:
            #self._change_state(ImplState.TAGS_WAIT)
        #else:
            #self._become_pending()

    #def _become_pending(self):
        #assert not self.wait_dep_tags
        #assert self.state != ImplState.RUNNING

        #if not self.jobs: # FIXME and not self._graph_executor.dont_run_new_jobs:
            #state = ImplState.SUCCESSFULL
        #else:
            #state = ImplState.RUNNING

        #self._change_state(state)

    def _change_state(self, state):
        with self.lock:
            #if state == self.state:
                #logging.warning("packet %s useless state change to current %s" % (self.id, state))
                #return

            #if not self._can_change_state(state):
                #raise NotAllowedStateChangeError(
                    #"packet %s\tincorrect state change request %r => %r" \
                        #% (self.name, self.state, state))

            self.state = state
            logging.debug("packet %s\tnew impl state %r", self.name, state)

            self._update_repr_state()

            if self.queue:
                self.queue._on_packet_state_change(self)

            if state == ImplState.HISTORIED:
                self.FireEvent("change") # PacketNamesStorage

            if state in [ImplState.ERROR, ImplState.BROKEN]:
                self._send_email_on_error_state()

            elif state == ImplState.PENDING:
                self._create_place_if_need()

            elif state == ImplState.SUCCESSFULL:
                if self.done_tag:
                    self.done_tag.Set()

            if state in [ImplState.SUCCESSFULL, ImplState.HISTORIED, ImplState.BROKEN]:
                assert self._graph_executor.is_null() # FIXME
                self._release_place()

    def _update_repr_state(self):
        new = self._calc_repr_state(self.state)

        if new == self._repr_state:
            return

        self._repr_state = new
        self.history.append((new, time.time()))
        logging.debug("packet %s\tnew state %r", self.name, new)

        # TODO logging.debug("packet %s\twaiting for %s sec", self.name, delay)


    def destroy(self):
        if self.destroying:
            return

        g = self._graph_executor

        if not(g.is_null() or self.do_not_run):
            raise NonDestroyingStateError()

        self.destroying = True

        if not g.is_null():
            g.cancel()

        self._update_state()

    def rpc_remove(self):
        with self.lock:
            try:
                self.destroy()
            except NonDestroyingStateError:
                raise RpcUserError(RuntimeError("Can't remove packet in %s state" % self._repr_state))

    def _on_graph_executor_state_change(self):
        state = self._graph_executor.state

        if state == GraphState.SUCCESSFULL:
            assert self._graph_executor.is_null()
            self.finish_status = True
# TODO XXX
#           We set DummyGraphExecutor in any case here, but need to store
#           ._prev_snapshot_resource_id in self
#           XXX NO NO NO Can't do this in TIME_WAIT!
# TODO XXX
            self._graph_executor = DummyGraphExecutor()

        elif state == GraphState.ERROR:
            self.finish_status = False

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
                raise NotAllFileLinksAlive("Not all links alive")

            try:
                self.directory = None
                self._create_place(ctx)
            except:
                reraise("Failed to resurrect directory")

    def _try_recover_after_backup_loading(self, ctx):
        if self.state in [ImplState.BROKEN, ImplState.HISTORIED]:
            if self.directory:
                self._release_place()

        # Can exists only in tempStorage or in backups wo locks+backup_in_child
        elif self.state == ImplState.UNINITIALIZED:
            raise RuntimeError("Can't restore packets in UNINITIALIZED state")
# TODO TODO
# TODO TODO
# TODO TODO
# TODO TODO
        #elif self.state == ReprState.NONINITIALIZED: # XXX FIXME TODO
            #self._recover_noninitialized(ctx) # XXX FIXME TODO

        if self.directory:
            self._try_recover_directory(ctx)
        else:
            directory = os.path.join(ctx.packets_directory, self.id)
            if os.path.isdir(directory):
                shutil.rmtree(directory, onerror=None)

        self._graph_executor.recover_after_backup_loading()

        self._update_state()

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
        #self._create_place_if_need()

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
            if not ctx.send_emergency_emails and self.state == ImplState.BROKEN:
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
                if self.state != ImplState.HISTORIED: # FIXME
                    self._process_tag_set_event(ref)

    def rpc_add_job(self, shell, parents, pipe_parents, set_tag, tries,
                    max_err_len, retry_delay, pipe_fail, description, notify_timeout,
                    max_working_time, output_to_status):
        with self.lock:
            if self.queue or not self._is_executable():
                raise RpcUserError(RuntimeError("Can't add jobs in %s state" % self._repr_state))
            #if self.state != ImplState.UNINITIALIZED: # TODO
                #raise RpcUserError(RuntimeError("incorrect state for \"Add\" operation: %s" % self.state))

        # FIXME TODO
            if isinstance(self, SandboxPacket) and max_err_len and max_err_len > DEFAULT_JOB_MAX_ERR_LEN:
                max_err_len = DEFAULT_JOB_MAX_ERR_LEN

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

    def _resume(self, resume_workable=False):
        raise AssertionError("Diese Funktion ist Wunderwaffe")

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
            if self._repr_state == ReprState.WAITING else None

        all_tags = list(self.all_dep_tags)

        status = dict(name=self.name,
# TODO
                      sandbox_task_id=None,

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

        if self.state == ImplState.BROKEN:
            extra_flags.add("can't-be-recovered")

        if self._repr_state == ReprState.SUSPENDED and self.state == ImplState.RUNNING:
            extra_flags.add("manually-suspended")

        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

    # XXX XXX WTF XXX XXX
        # FIXME WHY? no: HISTORIED, NONINITIALIZED, CREATED
        #if self.state not in [ImplState.HISTORIED, ImplState.UNINITIALIZED]:
        #if self._repr_state in (ReprState.ERROR, ReprState.SUSPENDED,
                          #ReprState.WORKABLE, ReprState.PENDING,
                          #ReprState.SUCCESSFULL, ReprState.WAITING):
        #if self._graph_executor:
        status["jobs"] = self._graph_executor.produce_detailed_status()

        return status

    def _check_add_files(self):
        if self._is_executable() or self.do_not_run:
            return

        raise RpcUserError(RuntimeError("Can't add files/resources in %s state" % self._repr_state))

    def rpc_add_binary(self, binname, file):
        with self.lock:
            self._check_add_files()
            self.files_modified = True
            self._add_link(binname, file)

    def rpc_suspend(self, kill_jobs=False):
        with self.lock:
            #if self.state == ImplState.BROKEN: # legacy check
                #raise RpcUserError(
                    #RuntimeError(
                        #"Can't suspend ERROR'ed packet %s with RCVR_ERROR flag set" % self.id))

            #raise NotImplementedError("CHECKS") # TODO XXX

# TODO XXX
            if self.state in [
                ImplState.SUCCESSFULL,
                ImplState.BROKEN,
                ImplState.DESTROYING,
                ImplState.HISTORIED
            ]:
                raise RuntimeError()

            self.finish_status = None # for ERROR
# TODO XXX

            self.do_not_run = True

            self._do_graph_suspend(kill_jobs)

            self._update_state()

    def _try_create_place_if_need(self):
        try:
            self._create_place_if_need()
            return True
        except Exception:
            logging.exception("Failed to create place")
            #self._change_state(ImplState.BROKEN)
            self.is_broken = True
            self._graph_executor.cancel()
            self._update_state()
            return False

    def rpc_resume(self):
        with self.lock:
            #raise NotImplementedError("CHECKS") # TODO XXX

            if not self.do_not_run:
                return

            self.do_not_run = False

            self._do_graph_resume()
            self._share_files_as_resource_if_need()

            self._update_state()

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
            yield self.done_tag, self.state == ImplState.SUCCESSFULL

        for job_id, tag in self.job_done_tag.iteritems():
            yield tag, job_id in self.succeed_jobs

    def OnReset(self, (ref, comment)):
        if isinstance(ref, TagBase):
            self._on_tag_reset(ref, comment)

    def _reset(self):
        self._do_graph_reset()
        self.finish_status = None

    def rpc_reset(self, suspend=False):
        with self.lock:
            if not self._is_executable():
                return

            self._update_done_tags(lambda tag, _: tag.Unset())

            if not self.queue:
                return # noop

# XXX raise NotImplementedError("CHECKS") # TODO XXX

            self.do_not_run = suspend

            self._reset()
            self._share_files_as_resource_if_need()

            self._update_state()

    def _on_tag_reset(self, ref, comment):
        with self.lock:
            if not self._is_executable():
                return
            #if self.destroying:
                #return

            # FIXME Don't even update .wait_dep_tags if not self.is_resetable
            tag_name = ref.GetFullname()
            self.wait_dep_tags.add(tag_name)

            #if self.state == ImplState.UNINITIALIZED:
            if not self.queue:
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
            self._update_done_tags(
                lambda tag, is_done: tag.Reset(comment) if is_done else tag.Unset())

            self._reset()

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

    def _set_real_graph_executor_if_need(self):
        if isinstance(self._graph_executor, DummyGraphExecutor):
            self._graph_executor = self._create_job_graph_executor()
            self._graph_executor.init() # TODO Better

    def _move_to_queue(self, src_queue, dst_queue, from_rpc=False):
        with self.lock:
            self._check_can_move_beetwen_queues()

            if src_queue:
                src_queue._detach_packet(self)

            if dst_queue:
                # after _create_job_graph_executor() because of .has_pending_jobs()
                dst_queue._attach_packet(self)

                #self._graph_executor.set_runner(self.queue.runner)

            else:
                self._graph_executor = DummyGraphExecutor()

    def rpc_move_to_queue(self, src_queue, dst_queue):
        with self.lock:
            if not self.queue:
                raise RpcUserError(RuntimeError("Packet not yet attached to queue to call move"))

            if self.destroying:
                raise RpcUserError(RuntimeError("Packet is destroying"))

            if not self._graph_executor.is_null():
                raise RpcUserError(RuntimeError("Can't move packets with running jobs"))

            self._move_to_queue(src_queue, dst_queue, from_rpc=True)

    def _attach_to_queue(self, queue):
        with self.lock:
            if self.queue:
                raise
            if self.destroying:
                raise # FIXME j.i.c
            #assert self.state == ImplState.UNINITIALIZED
            self._move_to_queue(None, queue)

            self._share_files_as_resource_if_need()

            #self._become_pending_or_wait_tags()
            self._update_state()

    def make_job_graph(self):
        # Without deepcopy self.jobs[*].{tries,result} will be modifed
        return JobGraph(copy.deepcopy(self.jobs), self.kill_all_jobs_on_error)


class _LocalPacketJobGraphOps(object):
    def __init__(self, pck):
        self.pck = pck

    def get_io_directory(self):
        return self.pck.directory

    get_working_directory = get_io_directory

    def on_state_change(self):
        self.pck._on_graph_executor_state_change()

    def stop_waiting(self, stop_id):
        self.pck._stop_waiting(stop_id)

    def del_working(self, job):
        self.pck.queue._on_job_done(self)

    def add_working(self, job):
        self.pck.queue._on_job_get(self)

    #def set_successfull_state(self):
        ##self.pck._change_state(ImplState.SUCCESSFULL)
        #self.finish_status = True
        #self._update_state()

    #def set_errored_state(self):
        ##self.pck._change_state(ImplState.ERROR)
        #self.finish_status = False
        #self._update_state()

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
        self.pck.send_job_long_execution_notification(job)

    def start_process(self, *args, **kwargs):
        ctx = PacketCustomLogic.SchedCtx
        return ctx.run_job(*args, **kwargs)


#class NotAllowedStateChangeError(AssertionError):
    #pass

class NotAllFileLinksAlive(RuntimeError):
    pass

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

    #def _can_run_jobs_right_now(self):
        #return self.state in [ImplState.RUNNING, ImplState.PENDING] \
            #and self._graph_executor.has_jobs_to_run()

    def get_job_to_run(self):
        with self.lock:
            #if not self._can_run_jobs_right_now():
            if self.state not in [ImplState.RUNNING, ImplState.PENDING]:
                raise NotWorkingStateError("Can't run jobs in % state" % self._repr_state)

            self._set_real_graph_executor_if_need()

            runner = self._graph_executor.get_job_to_run()
            self._update_state() # FIXME in JobGraphExecutor

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
# XXX TODO
# Адовый костыль
# XXX TODO
            if self.state != ImplState.SUCCESSFULL:
                self._graph_executor.apply_jobs_results()

    def _create_job_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)

    def _stop_waiting(self, stop_id):
        with self.lock:
            self._graph_executor.stop_waiting(stop_id)

    def _check_can_move_beetwen_queues(self):
        return # TODO
        #if self._graph_executor.has_running_jobs():
            #raise as_rpc_user_error(
                #from_rpc, RuntimeError("Can't move packets with running jobs"))

    def has_pending_jobs(self):
        with self.lock:
            return self.state in [ImplState.PENDING, ImplState.RUNNING] \
                and bool(self._graph_executor.state & GraphState.PENDING_JOBS)

    def _do_graph_suspend(self, kill_jobs):
        if kill_jobs:
            self._graph_executor.cancel()

    def _do_graph_resume(self):
        self._graph_executor.reset_tries() # XXX

    def _do_graph_reset(self):
        self._graph_executor.reset()


class SandboxPacket(PacketBase):
    def _do_graph_suspend(self, kill_jobs):
        g = self._graph_executor
        if not g.is_null(): # null in ERROR state
            g.stop(kill_jobs)

    def _do_graph_resume(self):
        g = self._graph_executor

        if not g.is_null() and not (self.files_modified or self.resources_modified):
            g.try_soft_resume() # XXX reset_tries
            # if fail -- when g._remote_packet becomes None
            #         -- SandboxPacket becomes PENDING

    def _do_graph_reset(self):
        g = self._graph_executor

        if not self.wait_dep_tags and not g.is_null() and not (self.files_modified or self.resources_modified):
            g.try_soft_restart()
        else:
            g.reset()

    def _share_files_as_resource_if_need(self):
        with self.lock:
            if self.queue and self.files_modified and not self.files_sharing:
                self._share_files_as_resource()

    def _share_files_as_resource(self):
        with self.lock:
            ctx = self._get_scheduler_ctx()
            sharer = ctx.sandbox_resource_sharer

# TODO XXX FIXME Not garanteed anyhow for now
            assert self.directory

            # Force REMOTE_COPY_RESOURCE to create directory even if len(self.bin_links) == 1
            with open(self.directory + '/.force_multiple_files', 'w'):
                pass

            self.files_sharing = sharer.share(
                'REM_JOBPACKET_ADDED_FILE_SET',
                name='__auto_shared_files__',
                directory=self.directory,
                #files=self.bin_links.keys()
# XXX XXX XXX Doesn't work correctly if shared directory contains single file
                files=['.']
            )

            self.files_modified = False
            self.files_sharing.subscribe(self._on_files_shared)

            self._update_state()

    def _on_files_shared(self, future):
        with self.lock:
            self.files_sharing = None

            if self.files_modified:
                self._share_files_as_resource()
                return

# TODO XXX Handle exception
            self.shared_files_resource_id = future.get()

            self._update_state()

    def run(self, guard):
        with self.lock:
            if self.state != ImplState.PENDING:
            #if not self._is_executable():
                raise NotWorkingStateError("Can't run jobs in % state" % self._repr_state)

            self._set_real_graph_executor_if_need()

            self.resources_modified = False

            self._graph_executor.start(guard)

            self._update_state()

    def rpc_add_resource(self, name, path):
        with self.lock:
            PacketBase._check_add_files(self)
            self.resources_modified = True
            self.sbx_files[name] = path

    def _create_job_graph_executor(self):
        assert not self.files_modified

        files = self.sbx_files.copy()

        if self.shared_files_resource_id:
            prefix = 'sbx:%d/' % self.shared_files_resource_id

            for filename in self.bin_links.keys():
                files[filename] = prefix + filename

        logging.debug('_create_job_graph_executor(%s): %s' % (self.id, files))

        return sandbox_remote_packet.SandboxJobGraphExecutorProxy(
            sandbox_remote_packet.SandboxPacketOpsForJobGraphExecutorProxy(self),
            self.id,
            self.make_job_graph(),
            files,
            #self.make_sandbox_task_params()
        )

    def _check_can_move_beetwen_queues(self):
        pass

# DEBUG
    def create_sandbox_packet(self):
        return sandbox_packet.Packet(self.id, self.make_job_graph())

    def _check_add_files(self):
        PacketBase._check_add_files(self)

        if not self._get_scheduler_ctx().allow_files_auto_sharing:
            raise RpcUserError(RuntimeError("Can't add files to SandboxPacket"))
