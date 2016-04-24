# -*- coding: utf-8 -*-
from __future__ import with_statement
import tempfile
import os
import time
import shutil
import errno

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
    ACTIVE        = '_ACTIVE'
    SUCCESSFULL   = '_SUCCESSFULL'
    ERROR         = '_ERROR'
    BROKEN        = '_BROKEN'
    DESTROYING    = '_DESTROYING'
    HISTORIED     = '_HISTORIED'

    allowed = {
        UNINITIALIZED: [BROKEN, WAIT_TAGS, ACTIVE, DESTROYING, HISTORIED],
        WAIT_TAGS:     [BROKEN, ACTIVE, DESTROYING, HISTORIED], # + SUCCESSFULL
        ACTIVE:        [BROKEN, WAIT_TAGS, SUCCESSFULL, ERROR], # + DESTROYING, HISTORIED (see _can_change_state)
        SUCCESSFULL:   [BROKEN, ACTIVE, DESTROYING, HISTORIED, WAIT_TAGS],
        ERROR:         [BROKEN, ACTIVE, DESTROYING, HISTORIED, WAIT_TAGS],
        BROKEN:        [DESTROYING, HISTORIED],
        DESTROYING:    [HISTORIED],
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


class NotAllowedStateChangeError(AssertionError):
    pass


class PacketCustomLogic(object):
    SchedCtx = None

    @classmethod
    def UpdateContext(cls, context):
        cls.SchedCtx = context


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

        elif state in ImplState.DESTROYING:
            return ReprState.NONINITIALIZED # XXX FIXME

        elif state in ImplState.HISTORIED:
            return ReprState.HISTORIED

        elif state == ImplState.SUCCESSFULL:
            return ReprState.SUCCESSFULL

        elif state == ImplState.WAIT_TAGS:
            return ReprState.SUSPENDED

        assert state == ImplState.ACTIVE

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
                if self._impl_state == ImplState.WAIT_TAGS:
                    self._begin_execute()
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
            self.binLinks[binname] = file.checksum

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

        if src == ImplState.ACTIVE and dst == ImplState.DESTROYING:
            return self._graph_executor.is_stopped()

        elif src == ImplState.ACTIVE and dst == ImplState.HISTORIED:
            return self._graph_executor.is_null()

        elif src == ImplState.UNINITIALIZED and dst == ImplState.SUCCESSFULL:
            return not self.jobs and not self.wait_dep_tags

        elif src == ImplState.WAIT_TAGS and dst == ImplState.SUCCESSFULL:
            return not self.jobs

        return dst in ImplState.allowed[src]

    def _begin_execute_or_wait_tags(self):
        if self.wait_dep_tags:
            self._change_state(ImplState.WAIT_TAGS)
        else:
            self._begin_execute()

    def _begin_execute(self):
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

            if state == ImplState.ACTIVE:
                self._graph_executor.restart()

            elif state in [ImplState.ERROR, ImplState.BROKEN]:
                self._send_email_on_error_state()

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

        self._on_repr_state_change()

    def is_broken(self):
        return self._impl_state == ImplState.BROKEN

    def rpc_remove(self):
        with self.lock:
            if self._impl_state == ImplState.HISTORIED:
                return
            try:
                self.destroy()
            except NotAllowedStateChangeError:
                raise RpcUserError(RuntimeError("Can't remove packet in %s state" % self.state))

    def destroy(self):
        with self.lock:
            g = self._graph_executor

            g.reset()

            self._change_state(
                ImplState.DESTROYING
                    if g.need_indefinite_time_to_reset()
                    else ImplState.HISTORIED
            )

    def _on_job_graph_become_empty(self):
        with self.lock:
            if self._impl_state == ImplState.DESTROYING:
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
            self._add_link(binname, file)

    def rpc_suspend(self, kill_jobs=False):
        with self.lock:
            if self._impl_state == ImplState.BROKEN: # legacy check
                raise RpcUserError(
                    RuntimeError(
                        "Can't suspend ERROR'ed packet %s with RCVR_ERROR flag set" % self.id))

            self._graph_executor.disallow_to_run_jobs(kill_running=kill_jobs)

    def rpc_resume(self):
        with self.lock:
            # `reset_tries' is legacy behaviour of `rpc_resume'
            self._graph_executor.allow_to_run_jobs(reset_tries=True)

    def _reset(self, tag_op):
        def update_tags():
            for tag, is_done in self._get_all_done_tags():
                tag_op(tag, is_done)

        # XXX force call to _change_state for packets without jobs
        #if self._clean_state and self._graph_executor.jobs:
            #update_tags()
            #return

        # if not(self._graph_executor.jobs) we will first reset, then set (by _create_place_if_need)

        # TODO Test that job_done_tag are reset
        update_tags()

        if self.wait_dep_tags:
            self._graph_executor.reset()

        if not self._try_create_place_if_need():
            self._change_state(ImplState.BROKEN)
            self._graph_executor.reset()
            return

        self._begin_execute_or_wait_tags()

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
            if suspend:
                self._graph_executor.disallow_to_run_jobs()

            self._reset(lambda tag, _: tag.Unset())

            if not suspend:
                self._graph_executor.allow_to_run_jobs()

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
            self._begin_execute_or_wait_tags()

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
        self.pck.FireEvent("job_done", job) # queue.working.discard(job)

    def add_working(self, job):
        self.pck.FireEvent("job_get", job) # queue.working.add(job)

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


class LocalPacket(PacketBase):
    INCORRECT = object()

    def _create_job_graph_executor(self):
        return job_graph.JobGraphExecutor(
            _LocalPacketJobGraphOps(self), # TODO Cyclic reference
            self.id,
            self.make_job_graph(),
        )

    def _on_repr_state_change(self):
        self.FireEvent("change") # queue.relocatePacket

    def _can_run_jobs_right_now(self):
        return self._impl_state == ImplState.ACTIVE \
            and self._graph_executor.can_run_jobs_right_now()

    def get_job_to_run(self):
        with self.lock:
            if not self._can_run_jobs_right_now():
                return self.INCORRECT

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


class _SandboxPacketJobGraphExecutorProxyOps(object):
    def __init__(self, pck):
        self.pck = pck

    def update_repr_state(self):
        self.pck._update_repr_state()

    def set_successfull_state(self):
        self.pck._change_state(ImplState.SUCCESSFULL)

    def set_errored_state(self):
        self.pck._change_state(ImplState.ERROR)

    def job_done_successfully(self, job_id):
        tag = self.pck.job_done_tag.get(job_id)
        if tag:
            tag.Set()

    def create_job_runner(self, job):
        raise AssertionError()

    def on_job_graph_becomes_empty(self):
        self.pck._on_job_graph_become_empty()

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


class RemotePacketsDispatcher(object):
    def __init__(self, listen_port):
        self.listen_port = listen_port
        self.local_hostname = ...
        self.packets = {}

    def start(self):
        self._rpc_server = self.RpcServer(self, listen_port)
        self.sandbox = MagicAsyncRetriableSandbox(...)

    def stop(self):
        raise NotImplementedError()

    def register_packet(self, pck):
        with self.lock:
            pck.state = WAIT_TASK_CREATION
            pck.task_creation_future = self.sandbox.create_task(...)
            self.packets[pck.id] = pck

            # create
        # indef

            # update
        # indef

            # start
# from now on we must do both:
# 1. check task status (maybe not from now actually)
# 2. be ready for rpc requests
        # indef

            # really started on some host
        # indef

            # we get know that it started on some host

    def cancel_packet(self, pck):
        raise NotImplementedError()


class SandboxRemotePacket(object):
    def __init__(self, ops, pck_id, graph, snapshot_resource_id, sandbox_task_params)
        self.id = (pck_id, time.time())
        self._ops = ops
        self._task_creation_future

        raw_snapshot = None
        if not snapshot_resource_id:
            raw_snapshot = self._produce_raw_snapshot(pck_id, graph)

        remote_packets_dispatcher.register_packet(self,
                            raw_snapshot=raw_snapshot,
                            snapshot_resource_id=snapshot_resource_id,
                            sandbox_task_params=sandbox_task_params)

    def send(self, msg):
        remote_packets_dispatcher.send(self, msg)

    def destroy(self):
        remote_packets_dispatcher.destroy(self)

    @staticmethod
    def _produce_raw_snapshot(pck_id, graph):
        pck = sandbox_packet.Packet(pck_id, graph)
        return base64.b64encode(cPickle.dumps(pck, 2))


class SandboxJobGraphExecutorProxy(object):
    def __init__(self, ops, pck_id, graph, sandbox_task_params):
        self._ops = ops
        self._graph = graph
        self.pck_id = pck_id
        self._remote_packet = None
        self._suspended_snapshot_resource_id = None
        self._sandbox_task_params = sandbox_task_params

    # XXX
        self.__dont_run_new_jobs = False
        self.__must_be_running = True # FIXME
        self.detailed_status = None
        self.state = ReprState.PENDING
        #self.history = [] # XXX TODO Непонятно вообще как с этим быть

    def _create_remote_packet(self):
        return SandboxRemotePacket(
            self, # Ops?
            self.pck_id,
            self._graph,
            self._suspended_snapshot_resource_id,
            self._sandbox_task_params
        )

    def get_repr_state(self):
        return self.state

    def produce_detailed_status(self):
        return self.detailed_status

########### Ops for _remote_packet
    def on_packet_terminated(self, how?):
        with self._ops.lock: # FIXME lazy@ How do it right?
            self._remote_packet = None
            if self.__must_be_running and not self.__dont_run_new_jobs: # FIXME
                self._remote_packet = self._create_remote_packet()
            else:
                if ...:
                    self._suspended_snapshot_resource_id = how....
                else:
                    self._suspended_snapshot_resource_id = None
                self._ops.on_job_graph_becomes_empty()

    def on_..._update(self, update):
        with self._ops.lock:
            self.....

###########
    def disallow_to_run_jobs(self, kill_running=False):
        with self._ops.lock:
            self.__dont_run_new_jobs = True
            if self._remote_packet:
                self._remote_packet.disallow_to_run_jobs(kill_running)

    def allow_to_run_jobs(self, reset_tries=False):
        with self._ops.lock:
            self.__dont_run_new_jobs = False
            if self._remote_packet:
                self._remote_packet.allow_to_run_jobs(reset_tries)

    def restart(self):
        with self._ops.lock:
            self.__must_be_running = True
            if self._remote_packet:
                self._remote_packet.restart()
            else:
                self._remote_packet = self._create_remote_packet()

    def reset(self):
        with self._ops.lock:
            self.__must_be_running = False
            if self._remote_packet:
                self._remote_packet.destroy()

    def is_stopped(self):
        with self._ops.lock:
            return not self._remote_packet or self._remote_packet.is_suspended()

    def is_null(self):
        return not self._remote_packet

    def need_indefinite_time_to_reset(self):
        return bool(self._remote_packet)
###########

    def recover_after_backup_loading(self):
        pass # FIXME

    #def on_job_done(self, runner):
    #def apply_jobs_results(self):
    #def vivify_jobs_waiting_stoppers(self):
    #def stop_waiting(self, stop_id):
    #def create_job_file_handles(self, job):
    #def get_working_jobs(self):
    #def get_job_to_run(self):
    #def can_run_jobs_right_now(self):


class SandboxPacketOpsForJobGraphExecutorProxy(object):
    def __init__(self, pck):
        self._pck = pck
        self.lock = pck.lock


class SandboxPacket(PacketBase):
    def _create_job_graph_executor(self):
        return SandboxJobGraphExecutorProxy(
            SandboxPacketOpsForJobGraphExecutorProxy(self),
            self.id,
            self.make_job_graph(),
            self.make_sandbox_task_params()
        )

    #def process_incoming(self):
        #with self.lock:
            #self._graph_executor.process_incoming()

    def _apply_sandbox_message(self, msg):
        self._messages.append(msg)
        with self.lock:
            self

    def _on_repr_state_change(self):
        pass

    def _check_can_move_beetwen_queues(self):
        pass

    def rpc_add_binary(self, binname, file):
        #self._get_scheduler_ctx().sandbox_files.add(file.path, checksum=file.checksum)
        super(SandboxPacket, self).rpc_add_binary(binname, file)

# DEBUG
    def create_sandbox_packet(self):
        return sandbox_packet.Packet(self.id, self.make_job_graph())


# For loading legacy backups
class JobPacket(Unpickable(lock=PickableRLock,
                           jobs=dict,
                           edges=dict, # jobs_graph
                           done=set, # succeed_jobs
                           leafs=set, # jobs_to_run
                           #_active_jobs=always(_ActiveJobs),
                           job_done_indicator=dict, # job_done_tag
                           waitingDeadline=int, # REMOVED
                           allTags=set, # all_dep_tags
                           waitTags=set, # wait_dep_tags
                           binLinks=dict, # bin_links
                           state=(str, ReprState.ERROR),
                           history=list,
                           notify_emails=list,
                           flags=int, # REMOVED
                           kill_all_jobs_on_error=(bool, True),
                           _clean_state=(bool, False), # False for loading old backups
                           isResetable=(bool, True), # is_resetable
                           notify_on_reset=(bool, False),
                           notify_on_skipped_reset=(bool, True),
                           directory=lambda *args: args[0] if args else None,
                           as_in_queue_working=always(set), # active_jobs_cache
                           # + jobs_to_retry
                           # + failed_jobs
                           # + dont_run_new_jobs
                          ),
                CallbackHolder,
                ICallbackAcceptor
               ):

    class PacketFlag:
        USER_SUSPEND = 0b0001
        RCVR_ERROR   = 0b0010

    StateMap = {
        ReprState.CREATED:          ImplState.UNINITIALIZED,
        ReprState.NONINITIALIZED:   ImplState.ACTIVE,
        ReprState.WORKABLE:         ImplState.ACTIVE,
        ReprState.PENDING:          ImplState.ACTIVE,
        #ReprState.SUSPENDED:
        ReprState.WAITING:          ImplState.ACTIVE,
        ReprState.ERROR:            ImplState.ERROR,
        ReprState.SUCCESSFULL:      ImplState.SUCCESSFULL,
        ReprState.HISTORIED:        ImplState.HISTORIED,
    }

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("JobPacket constructor is private")

    def convert_to_v2(self):
        pckd = self.__dict__

        self.failed_jobs = set()
        if self.state == ReprState.ERROR:
# FIXME pop may throw
            self.failed_jobs.add(self.leafs.pop())

        self.jobs_to_retry = {}
        if self.state == ReprState.WAITING:
# FIXME pop may throw
            self.jobs_to_retry[1] = (self.leafs.pop(), None, self.waitingDeadline)
        pckd.pop('waitingDeadline', None)

        self.done_tag = pckd.pop('done_indicator')
        #self.jobs_graph = pckd.pop('edges')
        self.succeed_jobs = pckd.pop('done')
        self.jobs_to_run = pckd.pop('leafs')
        self.job_done_tag = pckd.pop('job_done_indicator')
        self.all_dep_tags = pckd.pop('allTags')
        self.wait_dep_tags = pckd.pop('waitTags')
        self.bin_links = pckd.pop('binLinks')
        self.is_resetable = pckd.pop('isResetable')
        self.active_jobs_cache = pckd.pop('as_in_queue_working')

        self.dont_run_new_jobs = bool(self.flags & self.PacketFlag.USER_SUSPEND)
        has_recovery_error = bool(self.flags & self.PacketFlag.RCVR_ERROR)
        pckd.pop('flags')

        if self.state == ReprState.SUSPENDED:
            self._impl_state = ImplState.WAIT_TAGS if self.wait_dep_tags else ImplState.ACTIVE
        else:
            self._impl_state = self.StateMap[self.state]

        if self._impl_state == ImplState.ERROR and has_recovery_error:
            self._impl_state = ImplState.BROKEN
