# -*- coding: utf-8 -*-
from __future__ import with_statement
import tempfile
import os
import time
import shutil
import errno
import sys
import copy
import re
#import zlib
#import cPickle as pickle

from callbacks import CallbackHolder, ICallbackAcceptor, TagBase, tagset
from common import BinaryFile, PickableRLock, Unpickable, safeStringEncode, as_rpc_user_error, RpcUserError
from job import Job, JobRunner
import osspec
import messages
from rem_logging import logger as logging
import job_graph
from job_graph import GraphState
from packet_state import PacketState
ImplState = PacketState
import sandbox_remote_packet
from sandbox_releases import MalformedResourceDescr

_SANDBOX_RELEASE_PATH_RE = re.compile('sbx:([^/]+)(/[-/_a-zA-Z0-9.?*]*)?$')
_INTEGER_RE = re.compile('^\d+$')


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


class JobGraph(object):
    def __init__(self, jobs, kill_all_jobs_on_error):
        self.jobs = jobs
        self.kill_all_jobs_on_error = kill_all_jobs_on_error


def reraise(msg):
    t, e, tb = sys.exc_info()
    raise RuntimeError, RuntimeError('%s: %s' % (msg, e)), tb


def always(ctor):
    def always(*args):
        return ctor()
    return always


# FIXME Use None instead of DummyGraphExecutor?

class DummyGraphExecutor(object):
    state = GraphState.PENDING_JOBS

    def is_null(self):
        return True

    def get_global_error(self):
        return None

    def produce_detailed_status(self):
        return None

    def get_working_jobs(self):
        return []

    def recover_after_backup_loading(self):
        pass

    def is_cancelling(self):
        return False

    def is_stopping(self):
        return False

    def vivify_jobs_waiting_stoppers(self):
        pass

    def cancel(self):
        pass

    def reset(self):
        pass

    def reset_tries(self):
        pass

    def get_worker_state(self):
        return None

    def has_jobs_to_run(self):
        raise NotImplementedError()


DUMMY_GRAPH_EXECUTOR = DummyGraphExecutor()


def _value_or_None(*args):
    return args[0] if args else None


class PacketBase(Unpickable(
                           lock=PickableRLock,

                           jobs=dict,

                           all_dep_tags=set,
                           wait_dep_tags=set,

                           job_done_tag=dict,
                           #done_tag=object,

                           bin_links=dict,
                           sbx_files=dict,
                           directory=_value_or_None,

                           _repr_state=(str, ReprState.ERROR),
                           state=(str, ImplState.BROKEN),

                           notify_emails=list,
                           is_resetable=(bool, True),
                           notify_on_reset=(bool, False),
                           notify_on_skipped_reset=(bool, True),
                           resolved_releases=dict,
                           unresolved_release_count=int,
                           last_sandbox_task_id=_value_or_None,

                           history=list,

                           do_not_run=bool,
                           destroying=bool,
                           is_broken=bool,

                           req_sandbox_host=_value_or_None,
                           user_labels=_value_or_None,
                          ),
                CallbackHolder,
                ICallbackAcceptor):

    # The legacy
    IMPL_TO_REPR_STATE_MAPPING = {
        ImplState.UNINITIALIZED:    ReprState.CREATED,
        ImplState.ERROR:            ReprState.ERROR,
        ImplState.BROKEN:           ReprState.ERROR,
        ImplState.PENDING:          ReprState.PENDING,
        ImplState.SUCCESSFULL:      ReprState.SUCCESSFULL,

        ImplState.SHARING_FILES:    ReprState.SUSPENDED,
        ImplState.RESOLVING_RELEASES: ReprState.SUSPENDED,

        ImplState.TAGS_WAIT:        ReprState.SUSPENDED,
        ImplState.PAUSED:           ReprState.SUSPENDED,
        ImplState.PAUSING:          ReprState.SUSPENDED,

        ImplState.RUNNING:          ReprState.WORKABLE,

        # Must be WORKABLE to support fast_restart (FIXME Don't remember why)
        ImplState.PREV_EXECUTOR_STOP_WAIT: ReprState.WORKABLE,

        ImplState.TIME_WAIT:        ReprState.WAITING,

        ImplState.DESTROYING:       ReprState.WORKABLE,
        ImplState.HISTORIED:        ReprState.HISTORIED,
    }

    def __init__(self, name, priority, context, notify_emails, wait_tags=(),
                 set_tag=None, kill_all_jobs_on_error=True, is_resetable=True,
                 notify_on_reset=False, notify_on_skipped_reset=True, sandbox_host=None,
                 user_labels=None):
        super(PacketBase, self).__init__()
        self.name = name
        self.id = None
        self.directory = None
        self.queue = None
        self.finish_status = None
        self._saved_jobs_status = None
        self.last_sandbox_task_id = None
        self.user_labels = user_labels

        self._graph_executor = DUMMY_GRAPH_EXECUTOR

        self._create_place(context)
        self.id = os.path.split(self.directory)[-1]

        self.files_modified = False
        self.resources_modified = False

        self.files_sharing = None
        self.shared_files_resource_id = None
        self.resolved_releases = {}
        self.unresolved_release_count = 0
        self.req_sandbox_host = sandbox_host

        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.priority = priority
        self.notify_emails = list(notify_emails)
        self.is_resetable = is_resetable
        self.notify_on_reset = notify_on_reset
        self.notify_on_skipped_reset = notify_on_skipped_reset
        self.done_tag = set_tag

        self.tags_awaited = not wait_tags
        self._set_waiting_tags(wait_tags)

        self._update_state()

    def _will_never_be_executed(self):
        return self.is_broken or self.destroying

    # TODO Test this code statically with mask
    def _calc_state(self):
        graph = self._graph_executor

        if self.destroying:
            if graph.is_cancelling():
                assert graph.state & GraphState.CANCELLED
                assert graph.state & GraphState.WORKING
                return ImplState.DESTROYING
            else:
                assert graph.is_null()
                return ImplState.HISTORIED

        elif self.is_broken:
            return ImplState.BROKEN # may be GraphState.WORKING

        elif not self.queue:
            return ImplState.UNINITIALIZED

        elif self.wait_dep_tags and not self.tags_awaited:
            return ImplState.TAGS_WAIT # may be GraphState.WORKING

        elif not self.jobs:
            return ImplState.SUCCESSFULL


# FIXME do_not_run vs. finish_status priority?

        elif self.do_not_run:
            if self._is_graph_stopping():
                assert graph.state & GraphState.WORKING, "Got %s" % graph.state
                return ImplState.PAUSING
            else:
                assert graph.is_null()
                return ImplState.PAUSED # Здесь можно обновлять файлы/ресурсы, но после этого...
                            # мы должны быть уверены, что Граф будет stop/start'ed,
                            # например, за счёт .files_modified/.resources_modified

        elif self.finish_status is not None:
            return ImplState.SUCCESSFULL if self.finish_status else ImplState.ERROR

    # FIXME
        elif graph.state == GraphState.TIME_WAIT:
            return ImplState.TIME_WAIT

        elif not graph.is_null():
            if graph.is_cancelling():
                assert graph.state & GraphState.WORKING
                assert graph.state & GraphState.CANCELLED # FIXME | SUSPENDED
                                                          # is_stopped contains SUSPENDED and CANCELLED
                return ImplState.PREV_EXECUTOR_STOP_WAIT

            else:
                assert graph.state & GraphState.WORKING
                return ImplState.RUNNING # WORKING ?| PENDING

        elif self.files_sharing:
            return ImplState.SHARING_FILES

        elif self.unresolved_release_count:
            return ImplState.RESOLVING_RELEASES

        else:
            return ImplState.PENDING

    def _update_state(self):
        new = self._calc_state()

        if self.state != new:
            self.state = new
            logging.debug("packet %s\tnew impl state %r", self.name, new)

            self._update_repr_state()

            self._on_state_change(new)
        #else:
            #self._update_repr_state() # ImplState.RUNNING: 1. PENDING, 2. WORKABLE

        if self.queue:
            self.queue.update_pending_jobs_state(self) # TODO only for LocalPacket actually

    def _calc_repr_state(self):
        #if self.state == ImplState.RUNNING:
            #return ReprState.PENDING if self._graph_executor.state & GraphState.PENDING_JOBS \
                #else ReprState.WORKABLE

        return self.IMPL_TO_REPR_STATE_MAPPING[self.state]

    def __getstate__(self):
        sdict = CallbackHolder.__getstate__(self)

        if sdict['done_tag']:
            sdict['done_tag'] = sdict['done_tag'].name

        job_done_tag = sdict['job_done_tag'] = sdict['job_done_tag'].copy()
        for job_id, tag in job_done_tag.iteritems():
            job_done_tag[job_id] = tag.name

        if sdict['files_sharing']:
            sdict['files_sharing'] = True
            sdict['files_modified'] = True

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
        if self._will_never_be_executed():
            return

        tag_name = tag.GetFullname()

        if tag_name in self.wait_dep_tags:
            self.wait_dep_tags.remove(tag_name)

            if not self.wait_dep_tags:
                self.tags_awaited = True
                self._share_files_as_resource_if_need()
                self._update_state()

    def vivify_done_tags_if_need(self, tagStorage):
        with self.lock:
            if isinstance(self.done_tag, str):
                self.done_tag = tagStorage.AcquireTag(self.done_tag)
            for jid, cur_val in self.job_done_tag.iteritems():
                if isinstance(cur_val, str):
                    self.job_done_tag[jid] = tagStorage.AcquireTag(cur_val)

    # XXX ctx.allow_files_auto_sharing is for testing purposes
    def vivify_resource_sharing(self):
        if self.files_sharing:
            raise NotImplementedError("Resource sharing vivify is not implemented")

    def vivify_jobs_waiting_stoppers(self):
        with self.lock:
            self._graph_executor.vivify_jobs_waiting_stoppers()

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

# TODO BROKEN
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

    def _on_state_change(self, state):
        with self.lock:
            if self.queue:
                self.queue._on_packet_state_change(self)

            if state == ImplState.HISTORIED:
                self.FireEvent("change") # PacketNamesStorage
                #self.queue = None

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
        new = self._calc_repr_state()

        if new == self._repr_state:
            return

        self._repr_state = new
        self.history.append((new, time.time()))
        logging.debug("packet %s\tnew state %r", self.name, new)

        if new == ReprState.WAITING:
            deadline = self._graph_executor.get_nearest_retry_deadline()
            delay = max(deadline - time.time(), 0) if deadline else None
            logging.debug("packet %s\twaiting for %s sec", self.name, delay)

    def _destroy(self):
        if self.destroying:
            return

        g = self._graph_executor

        if not(g.is_null() or self.do_not_run):
            raise NonDestroyingStateError()

        self.destroying = True

        if not g.is_null():
            g.cancel()

        self._update_state()

    def destroy(self):
        with self.lock:
            self._destroy()

    def rpc_remove(self):
        with self.lock:
            try:
                self._destroy()
            except NonDestroyingStateError:
                raise RpcUserError(RuntimeError("Can't remove packet in %s state" % self._repr_state))

    #@staticmethod
    #def _produce_compressed_job_status(graph_executor):
        #status = graph_executor.produce_detailed_status()
        #for job in status:
            #job['results'] = [
                #r.data for r in job['results']
            #]

        #return zlib.compress(pickle.dumps(status))

    def _on_graph_executor_state_change(self):
        state = self._graph_executor.state

        if state == GraphState.SUCCESSFULL:
            assert self._graph_executor.is_null()
            self.finish_status = True
            self._saved_jobs_status = self._graph_executor.produce_detailed_status()
            #self._saved_jobs_status = self._produce_compressed_job_status(self._graph_executor)
            self._graph_executor = DUMMY_GRAPH_EXECUTOR

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
        prev_packets_directory, _ = os.path.split(self.directory)
        actual_directory = os.path.join(ctx.packets_directory, self.id)

        if os.path.isdir(self.directory):
            if prev_packets_directory != ctx.packets_directory:
                try:
                    logging.debug("relocates directory %s to %s", self.directory, actual_directory)
                    shutil.copytree(self.directory, actual_directory)
                    self.directory = actual_directory
                except:
                    reraise("Failed to relocate directory")

        else:
            if os.path.isdir(actual_directory):
                self.directory = actual_directory
                return # FIXME Recreate?

            if not self._are_links_alive(ctx):
                raise NotAllFileLinksAlive("Not all links alive")

            try:
                self.directory = None
                self._create_place(ctx)
            except:
                reraise("Failed to resurrect directory")

    def _try_recover_after_backup_loading(self, ctx):
        self._graph_executor.recover_after_backup_loading()

        if self.state in [ImplState.BROKEN, ImplState.HISTORIED]:
            if self.directory:
                self._release_place()

        # Can exists only in tempStorage or in backups wo locks+backup_in_child
        elif self.state == ImplState.UNINITIALIZED:
            raise RuntimeError("Can't restore packets in UNINITIALIZED state")

        if self.directory:
            self._try_recover_directory(ctx)
        else:
            directory = os.path.join(ctx.packets_directory, self.id)
            if os.path.isdir(directory):
                shutil.rmtree(directory, onerror=None)

        self._update_state()

    def try_recover_after_backup_loading(self, ctx):
        descr = '[%s, directory = %s]' % (self, self.directory)

        try:
            self._try_recover_after_backup_loading(ctx)
        except Exception as e:
            logging.exception("Failed to recover packet %s" % descr)
            self._mark_as_failed_on_recovery()

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
                return messages.FormatLongExecutionWarning(ctx, self, job)

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
            if self.queue or self._will_never_be_executed():
                raise RpcUserError(RuntimeError("Can't add jobs in %s state" % self._repr_state))
            #if self.state != ImplState.UNINITIALIZED: # TODO
                #raise RpcUserError(RuntimeError("incorrect state for \"Add\" operation: %s" % self.state))

            if max_err_len and max_err_len > self.MAX_ERR_LEN:
                max_err_len = self.MAX_ERR_LEN

            parents = list(set(parents + pipe_parents))
            pipe_parents = list(set(pipe_parents))

            for dep_id in parents:
                if dep_id not in self.jobs:
                    raise RpcUserError(RuntimeError("No job with id = %s in packet %s" % (dep_id, self.pck_id)))

            job = Job(self.id, shell, parents, pipe_parents, max_try_count=tries,
                      max_err_len=max_err_len, retry_delay=retry_delay,
                      pipe_fail=pipe_fail, description=description,
                      notify_timeout=notify_timeout,
                      max_working_time=max_working_time,
                      output_to_status=output_to_status)

            self.jobs[job.id] = job

            if set_tag:
                self.job_done_tag[job.id] = set_tag

            return job

    def History(self):
        return self.history or []

    def _get_extended_state(self):
        g = self._graph_executor
        return (self.state, g.state, g.get_worker_state())

    def _produce_clean_jobs_status(self):
        return [
            dict(
                id=str(job.id),
                shell=job.shell,
                desc=job.description,
                state="suspended" if job.parents else "pending",
                results=[],
                parents=map(str, job.parents or []),
                pipe_parents=map(str, job.inputs or []),
                output_filename=None,
                wait_jobs=map(str, job.parents),
            )
                for jid, job in self.jobs.iteritems()
        ]

    # FIXME It's better for debug to allow this call from RPC without lock
    #       * From messages it's called under lock
    def Status(self):
        return self._status()

    def _status(self):
        history = self.History()
        total_time = history[-1][1] - history[0][1]
        wait_time = 0

        for ((state, start_time), (_, end_time)) in zip(history, history[1:] + [("", time.time())]):
            if state in (ReprState.SUSPENDED, ReprState.WAITING):
                wait_time += end_time - start_time

        result_tag = self.done_tag.name if self.done_tag else None

        waiting_time = None
        if self.state == ImplState.TIME_WAIT:
            deadline = self._graph_executor.get_nearest_retry_deadline()
            if deadline:
                waiting_time = max(int(deadline - time.time()), 0)
            else:
                logging.error("Packet %s in WAITING but has no get_nearest_retry_deadline" % self.id)

        all_tags = list(self.all_dep_tags)

        status = dict(name=self.name,
                      is_sandbox=isinstance(self, SandboxPacket),
                      last_sandbox_task_id=self.last_sandbox_task_id, # TODO History of tasks
                      last_global_error=self._graph_executor.get_global_error(),
                      resolved_releases=self.resolved_releases,
                      state=self._repr_state,
                      extended_state=self._get_extended_state(),
                      wait=list(self.wait_dep_tags),
                      all_tags=all_tags,
                      result_tag=result_tag,
                      priority=self.priority,
                      notify_emails=self.notify_emails,
                      history=history,
                      total_time=total_time,
                      wait_time=wait_time,
                      last_modified=history[-1][1],
                      waiting_time=waiting_time,
                      queue=self.queue.name if self.queue else None,
                      labels=self.user_labels,
                      )

        extra_flags = set()

        if self.is_broken:
            extra_flags.add("can't-be-recovered")

        if self.do_not_run:
            extra_flags.add("manually-suspended")

        if extra_flags:
            status["extra_flags"] = ";".join(extra_flags)

        if not self._is_dummy_graph_executor():
            jobs = self._graph_executor.produce_detailed_status() \
                or self._produce_clean_jobs_status()
        elif self._saved_jobs_status:
            jobs = self._saved_jobs_status
        else:
            jobs = self._produce_clean_jobs_status()

        status["jobs"] = jobs

        return status

    def _check_add_files(self):
        if not self._will_never_be_executed() or self.do_not_run:
            return

        raise RpcUserError(RuntimeError("Can't add files/resources in %s state" % self._repr_state))

    def rpc_add_binary(self, binname, file):
        with self.lock:
            self._check_add_files()
            self.files_modified = True
            self._add_link(binname, file)

    def rpc_suspend(self, kill_jobs=False):
        with self.lock:
            if self.state == ImplState.PAUSED:
                return

            # FIXME suspend in SUCCESSFULL may mean: don't execute packet after restart/reset

            if self.state in [
                ImplState.SUCCESSFULL,
                ImplState.BROKEN,
                ImplState.DESTROYING,
                ImplState.HISTORIED
            ]:
                raise RpcUserError(RuntimeError("Can't suspend in %s state" % self._repr_state))

            self.finish_status = None # for ImplState.ERROR
            self._saved_jobs_status = None

            # XXX Previous implementation in addition did something like this:
            # + self.tags_awaited = not self.wait_dep_tags

            self.do_not_run = True

            self._do_graph_suspend(kill_jobs)

            self._update_state()

    def _try_create_place_if_need(self):
        try:
            self._create_place_if_need()
            return True
        except Exception:
            logging.exception("Failed to create place")
            self.is_broken = True
            self._graph_executor.cancel()
            self._update_state()
            return False

    def rpc_resume(self):
        with self.lock:
            if not self.do_not_run:
                return

            self.do_not_run = False

            self._do_graph_resume()
            self._share_files_as_resource_if_need()

            self._update_state()

    def _update_done_tags(self, op):
        for tag, is_done in self._get_all_done_tags():
            op(tag, is_done)

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
        if self.wait_dep_tags:
            self.tags_awaited = False
        self._do_graph_reset()
        self.finish_status = None
        self._saved_jobs_status = None

    def rpc_reset(self, suspend=False):
        with self.lock:
            if self._will_never_be_executed():
                return

            self._update_done_tags(lambda tag, _: tag.Unset())

            if not self.queue:
                return # noop

            self.do_not_run = suspend

            self._reset()
            self._share_files_as_resource_if_need()

            self._update_state()

    def _on_tag_reset(self, ref, comment):
        with self.lock:
            if self._will_never_be_executed():
                return

            # Legacy behaviour: add in any case
            tag_name = ref.GetFullname()
            self.wait_dep_tags.add(tag_name)

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
            self._set_real_graph_executor()

    def _is_dummy_graph_executor(self):
        return isinstance(self._graph_executor, DummyGraphExecutor)

    def _set_real_graph_executor(self):
        self._saved_jobs_status = None # j.i.c.
        self._graph_executor = self._create_job_graph_executor()
        self._graph_executor.init() # Circular references

    def _move_to_queue(self, dst_queue, from_rpc=False):
        with self.lock:
            self._check_can_move_beetwen_queues()

            if self.queue:
                self.queue._detach_packet(self)

            if dst_queue:
                # after _create_job_graph_executor() because of .has_pending_jobs()
                dst_queue._attach_packet(self)
            else:
                self._graph_executor = DUMMY_GRAPH_EXECUTOR

    def rpc_move_to_queue(self, dst_queue):
        with self.lock:
            if not self.queue:
                raise RpcUserError(RuntimeError("Packet not yet attached to queue to call move"))

            if self.destroying:
                raise RpcUserError(RuntimeError("Packet is destroying"))

            if not self._graph_executor.is_null():
                raise RpcUserError(RuntimeError("Can't move packets with running jobs"))

            if self.queue == dst_queue:
                return

            self._move_to_queue(dst_queue, from_rpc=True)

    def _attach_to_queue(self, queue):
        with self.lock:
            if self.queue:
                raise RpcUserError(RuntimeError("Packet already attached to queue"))

            if self.destroying:
                raise RpcUserError(RuntimeError("Packet is destroying"))

            self._move_to_queue(queue)
            self._share_files_as_resource_if_need()

            self._update_state()

    def make_job_graph(self):
        # Without deepcopy self.jobs[*].{tries,result} will be modifed
        # FIXME Use something else but deepcopy
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
        self.pck.queue._on_job_done(self, job)

    def add_working(self, job):
        self.pck.queue._on_job_get(self, job)

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
    MAX_ERR_LEN = 2 ** 20

    def _create_job_graph_executor(self):
        return job_graph.JobGraphExecutor(
            _LocalPacketJobGraphOps(self), # TODO Cyclic reference
            self.id,
            self.make_job_graph(),
        )

    def has_pending_jobs(self):
        with self.lock:
            return self.state in [ImplState.PENDING, ImplState.RUNNING] \
                and bool(self._graph_executor.state & GraphState.PENDING_JOBS)

    def get_job_to_run(self):
        with self.lock:
            def _raise():
                raise NotWorkingStateError("Can't run jobs in % state" % self._repr_state)

            if self.state not in [ImplState.RUNNING, ImplState.PENDING]:
                _raise()

            self._set_real_graph_executor_if_need()

            if not self._graph_executor.has_jobs_to_run():
                _raise()

            runner = self._graph_executor.get_job_to_run()
            #self._update_state() called in JobGraphExecutor

            return runner

    # Called in queue under packet lock
    def _get_working_jobs(self):
        return self._graph_executor.get_working_jobs()

    def on_job_done(self, runner):
        self._graph_executor.on_job_done(runner)

        with self.lock:
            if not self._is_dummy_graph_executor():
                self._graph_executor.apply_jobs_results()

    def _create_job_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)

    def _stop_waiting(self, stop_id):
        with self.lock:
            self._graph_executor.stop_waiting(stop_id)

    def _check_can_move_beetwen_queues(self):
        pass

    def _do_graph_suspend(self, kill_jobs):
        if kill_jobs:
            self._graph_executor.cancel()

    def _do_graph_resume(self):
        self._graph_executor.reset_tries() # XXX

    def _do_graph_reset(self):
        self._graph_executor.reset()

    def _is_graph_stopping(self):
        return not self._graph_executor.is_null() and self.do_not_run

    def _share_files_as_resource_if_need(self):
        pass

    def vivify_release_resolving(self):
        pass


class SandboxPacket(PacketBase):
    MAX_ERR_LEN = 1024

    def _do_graph_suspend(self, kill_jobs):
        g = self._graph_executor
        if not g.is_null(): # null in ERROR state
            g.stop(kill_jobs)

    def _do_graph_resume(self):
        g = self._graph_executor

        #if not g.is_null() and not (self.files_modified or self.resources_modified):
            #g.try_soft_resume() # XXX reset_tries
            # if fail -- when g._remote_packet becomes None
            #         -- SandboxPacket becomes PENDING

        g.reset_tries()

    def _do_graph_reset(self):
        g = self._graph_executor

        # TODO
        #if not self.wait_dep_tags and not g.is_null() and not (self.files_modified or self.resources_modified):
            #g.try_soft_restart()
        #else:
            #g.reset()
        g.reset()

    def _is_graph_stopping(self):
        return self._graph_executor.is_stopping()

    # For production-ready:
    #   1. write rem.storages.sharer_cache around rem.resource_sharing
    #   2. share all small files of packet in single resource
    #      and big files in different resources
    def _share_files_as_resource_if_need(self):
        with self.lock:
            if self.queue and self.files_modified and not self.files_sharing and self.jobs:
                self._share_files_as_resource()

    def _share_files_as_resource(self):
        with self.lock:
            ctx = self._get_scheduler_ctx()
            sharer = ctx.sandbox_resource_sharer

            # FIXME Not guaranteed anyhow
            assert self.directory

            # Force REMOTE_COPY_RESOURCE to create directory even if len(self.bin_links) == 1
            with open(self.directory + '/.force_multiple_files', 'w'):
                pass

            self.files_sharing = sharer.share(
                'REM_JOBPACKET_ADDED_FILE_SET',
                name='__auto_shared_files__',
                directory=self.directory,
                files=['.'] # FIXME self.bin_links.keys()
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

            # TODO Handle errors
            self.shared_files_resource_id = future.get()
            self.resources_modified = True

            self._update_state()

    def run(self, guard):
        with self.lock:
            if self.state != ImplState.PENDING:
                raise NotWorkingStateError("Can't run jobs in % state" % self._repr_state)

            # TODO Better
            if self.resources_modified and not self._is_dummy_graph_executor():
                self._graph_executor.reset_tries() # FIXME Because of self.resources_modified
                self._graph_executor._custom_resources \
                    = self._produce_job_graph_executor_custom_resources()

            elif self._is_dummy_graph_executor():
                self._set_real_graph_executor()

            self.resources_modified = False

            self._graph_executor.start(guard)

            self._update_state()

    def rpc_add_resource(self, name, path):
        with self.lock:
            if not path.startswith('sbx:'):
                raise RpcUserError(ValueError())

            PacketBase._check_add_files(self)

            # Must comes first (contains check, may throw)
            self._resolve_releases_if_need(path)

            self.resources_modified = True
            self.sbx_files[name] = path
            self._update_state()

    def _resolve_releases_if_need(self, path):
        m = _SANDBOX_RELEASE_PATH_RE.match(path)
        if not m:
            raise RpcUserError(RuntimeError("Malformed resource path '%s'" % path))

        resource_descr = m.groups()[0]

        if _INTEGER_RE.match(resource_descr):
            return

        # TODO Use parsed hashable Request instead of string descr

        if resource_descr in self.resolved_releases:
            return

        try:
            resolve = self._start_resolve_release(resource_descr)
        except MalformedResourceDescr as e:
            raise RpcUserError(ValueError(str(e)))

        self.resolved_releases[resource_descr] = None
        self.unresolved_release_count += 1
        resolve()

    # force re-resolve
    def rpc_resolve_resources(self):
        with self.lock:
            for descr, res_id in self.resolved_releases.items():
                if res_id is not None:
                    resolve = self._start_resolve_release(descr)
                    self.unresolved_release_count += 1
                    self.resolved_releases[descr] = None
                    resolve()
            self.resources_modified = True
            self._update_state()

    def _on_release_resolved(self, resource_descr, f):
        with self.lock:
            if not f.is_success():
# TODO add error message to _status()
                self.is_broken = True
                self._update_state()
                return

            self.unresolved_release_count -= 1
            self.resolved_releases[resource_descr] = f.get()

            if not self.unresolved_release_count and self.state == ImplState.RESOLVING_RELEASES:
                self._update_state()

    def vivify_release_resolving(self):
        with self.lock:
            if self._will_never_be_executed():
                return

            to_resolve = [
                resource_descr for resource_descr, resource_id in self.resolved_releases.items()
                    if resource_id is None
            ]

            self.unresolved_release_count = len(to_resolve)

            if not to_resolve:
                return

            for resource_descr in to_resolve:
                self._start_resolve_release(resource_descr)()

    # Implemented in 2 stages because # parse stage may throw and we don't want
    # to modify self before it
    def _start_resolve_release(self, descr):
        ctx = self._get_scheduler_ctx()
        resolver = ctx.sandbox_release_resolver

        req = resolver.parse_resource_descr(descr)

        def resolve():
            # FIXME Use int result in resolve() if it ready
            resolver.resolve(req
                ).subscribe(lambda f: self._on_release_resolved(descr, f))

        return resolve

    def _produce_job_graph_executor_custom_resources(self):
        assert not self.files_modified

        files = {}
        resources = set()

        # FIXME Don't compute on each run?
        for filename, path in self.sbx_files.items():
            m = _SANDBOX_RELEASE_PATH_RE.match(path)

            resource_descr, inner_path = m.groups()

            if _INTEGER_RE.match(resource_descr):
                resource_id = int(resource_descr)
            else:
                resource_id = self.resolved_releases[resource_descr]

            path = 'sbx:%s%s' % (resource_id, inner_path or '')

            files[filename] = path
            resources.add(resource_id)

        if self.shared_files_resource_id:
            files = files.copy()

            prefix = 'sbx:%d/' % self.shared_files_resource_id

            for filename in self.bin_links.keys():
                files[filename] = prefix + filename

        return sandbox_remote_packet.PacketResources(files, list(resources))

    def _create_job_graph_executor(self):
        assert not self.files_modified
        resources = self._produce_job_graph_executor_custom_resources()

        logging.debug('_create_job_graph_executor(%s): %s' % (self.id, resources))

        return sandbox_remote_packet.SandboxJobGraphExecutorProxy(
            sandbox_remote_packet.SandboxPacketOpsForJobGraphExecutorProxy(self),
            self.id,
            self.make_job_graph(),
            resources,
            host=self.req_sandbox_host,
            #self.make_sandbox_task_params()
        )

    def _check_can_move_beetwen_queues(self):
        pass

    def _check_add_files(self):
        PacketBase._check_add_files(self)

        if not self._get_scheduler_ctx().allow_files_auto_sharing:
            raise RpcUserError(RuntimeError("Can't add files to SandboxPacket"))


from rem.packet_legacy import JobPacket
