#!/usr/bin/env python
from __future__ import with_statement

import os
import sys
import re
import select
import signal
import socket
import time
import threading
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib
import datetime
import multiprocessing
import signal
import argparse
import tempfile
import shutil
import subprocess

from rem import constants, osspec
from rem import traced_rpc_method
from rem import CheckEmailAddress, LocalPacket, SandboxPacket, Scheduler, ThreadJobWorker, TimeTicker, XMLRPCWorker
from rem import AsyncXMLRPCServer
from rem.profile import ProfiledThread
from rem.callbacks import ETagEvent
from rem.common import as_rpc_user_error, RpcUserError, NamedTemporaryDir
import rem.common
import rem.fork_locking
from rem.rem_logging import logger as logging
import rem.rem_logging as rem_logging
from rem.context import Context
import rem.subprocsrv as subprocsrv
import rem.sandbox
import rem.subprocsrv_fallback
import rem.job
import rem.delayed_executor as delayed_executor
import rem.resource_sharing

class DuplicatePackageNameException(Exception):
    def __init__(self, pck_name, serv_name, *args, **kwargs):
        super(DuplicatePackageNameException, self).__init__(*args, **kwargs)
        self.message = 'DuplicatePackageNameException: Packet with name %s already exists in REM[%s]' % (pck_name, serv_name)


class AuthRequestHandler(SimpleXMLRPCRequestHandler):
    def _dispatch(self, method, params):
        func = self.server.funcs.get(method)
        if not func:
            raise Exception('method "%s" is not supported' % method)
        username = self.headers.get("X-Username", "Unknown")
        log_level = getattr(func, "log_level", None)
        log_func = getattr(logging, log_level, None) if log_level else None
        if callable(log_func):
            log_func("RPC method (user: %s, host: %s): %s %r", username, self.address_string(), method, params)
        return func(*params)


_scheduler = None
_context = None


def readonly_method(func):
    func.readonly_method = True
    return func

def rpc_assert(cond, msg):
    if not cond:
        raise RpcUserError(AssertionError(msg))

def MakeDuplicatePackageNameException(pck_name):
    e = DuplicatePackageNameException(pck_name, _context.network_name)
    return RpcUserError(xmlrpclib.Fault(1, e.message))


@traced_rpc_method("info")
def create_packet(packet_name, priority, notify_emails, wait_tagnames, set_tag,
                  kill_all_jobs_on_error=True,
                  packet_name_policy=constants.DEFAULT_DUPLICATE_NAMES_POLICY,
                  resetable=True, notify_on_reset=False, notify_on_skipped_reset=True,
                  is_sandbox=False):

    if packet_name_policy & constants.DENY_DUPLICATE_NAMES_POLICY and _scheduler.packetNamesTracker.Exist(packet_name):
        raise MakeDuplicatePackageNameException(packet_name)
    if notify_emails is not None:
        rpc_assert(isinstance(notify_emails, list), "notify_emails must be list or None")
        for email in notify_emails:
            rpc_assert(CheckEmailAddress(email), "incorrect e-mail: " + email)
    wait_tags = [_scheduler.tagRef.AcquireTag(tagname) for tagname in wait_tagnames]
    pck_cls = SandboxPacket if is_sandbox else LocalPacket
    pck = pck_cls(packet_name, priority, _context, notify_emails,
                    wait_tags=wait_tags, set_tag=set_tag and _scheduler.tagRef.AcquireTag(set_tag),
                    kill_all_jobs_on_error=kill_all_jobs_on_error, is_resetable=resetable,
                    notify_on_reset=notify_on_reset,
                    notify_on_skipped_reset=notify_on_skipped_reset)
    _scheduler.RegisterNewPacket(pck, wait_tags)
    logging.info('packet %s registered as %s', packet_name, pck.id)
    return pck.id


def MakeNonExistedPacketException(pck_id):
    return RpcUserError(AttributeError("nonexisted packet id: %s" % pck_id))

@traced_rpc_method()
def pck_add_job(pck_id, shell, parents, pipe_parents, set_tag, tries,
                max_err_len=None, retry_delay=None, pipe_fail=False, description="", notify_timeout=constants.NOTIFICATION_TIMEOUT, max_working_time=constants.KILL_JOB_DEFAULT_TIMEOUT, output_to_status=False):
    pck = _scheduler.tempStorage.GetPacket(pck_id)
    if pck is not None:
        if isinstance(shell, unicode):
            shell = shell.encode('utf-8')
        parents = map(int, parents)
        pipe_parents = map(int, pipe_parents)
        job = pck.rpc_add_job(shell, parents, pipe_parents,
                              set_tag and _scheduler.tagRef.AcquireTag(set_tag),
                              tries, max_err_len, retry_delay, pipe_fail,
                              description, notify_timeout, max_working_time,
                              output_to_status)
        return str(job.id)
    raise MakeNonExistedPacketException(pck_id)


@traced_rpc_method("info")
def pck_addto_queue(pck_id, queue_name, packet_name_policy=constants.IGNORE_DUPLICATE_NAMES_POLICY):
    pck = _scheduler.tempStorage.PickPacket(pck_id)
    if not pck:
        raise MakeNonExistedPacketException(pck_id)

    packet_name = pck.name
    if packet_name_policy & (constants.DENY_DUPLICATE_NAMES_POLICY | constants.WARN_DUPLICATE_NAMES_POLICY) and _scheduler.packetNamesTracker.Exist(packet_name):
        raise MakeDuplicatePackageNameException(packet_name)

    _scheduler.AddPacketToQueue(queue_name, pck)


@traced_rpc_method("info")
def pck_moveto_queue(pck_id, src_queue, dst_queue):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        pck.rpc_move_to_queue(
            _scheduler.rpc_get_queue(src_queue),
            _scheduler.rpc_get_queue(dst_queue)
        )
        return
    raise MakeNonExistedPacketException(pck_id)


#########

@traced_rpc_method()
def get_safe_cloud_state():
    pass # TODO XXX


# FIXME Timeouts?

@readonly_method
@traced_rpc_method()
def check_tag(tagname, timeout=None):
    return _scheduler.tagRef._are_tags_set([tagname]).get(timeout)[tagname]

@traced_rpc_method("info")
def set_tag(tagname, timeout=None):
    return _scheduler.tagRef._modify_tag_unsafe(tagname, ETagEvent.Set).get(timeout)

@traced_rpc_method("info")
def unset_tag(tagname, timeout=None):
    return _scheduler.tagRef._modify_tag_unsafe(tagname, ETagEvent.Unset).get(timeout)

@traced_rpc_method()
def reset_tag(tagname, msg="", timeout=None):
    return _scheduler.tagRef._modify_tag_unsafe(tagname, ETagEvent.Reset, msg).get(timeout)

# TODO Fix cloud_client.update
def check_item_count(items):
    max_item_count = 100000
    if len(items) > max_item_count:
        raise RuntimeError("Can't update/lookup more than %d items (got %d)" % (max_item_count, len(items)))

@readonly_method
@traced_rpc_method()
def check_tags(tags):
    check_item_count(tags)
    return _scheduler.tagRef._are_tags_set(tags).get()

@readonly_method
@traced_rpc_method()
def lookup_tags(tags):
    check_item_count(tags)
    return _scheduler.tagRef._lookup_tags(tags).get()

@readonly_method
@traced_rpc_method()
# TODO Move impl to TagStorage
def get_tag_local_state(tag):
    tag = _scheduler.tagRef._GetTagLocalState(tag)
    if not tag:
        return None

    state = tag.__dict__.copy()

    # TODO Kosher
    ret = {
        'is_set': state['done']
    }

    if tag.IsCloud():
        for field in ['version']:
            ret[field] = state.get(field)

    return ret

@traced_rpc_method("info")
def update_tags(updates):
    check_item_count(updates)
    # TODO VERIFY `updates' HERE
    return _scheduler.tagRef._modify_tags_unsafe(updates).get() # TODO timeout

@traced_rpc_method("info")
def list_cloud_tags_masks():
    return _scheduler.tagRef.list_cloud_tags_masks()

@readonly_method
@traced_rpc_method()
def get_dependent_packets_for_tag(tagname):
    return _scheduler.tagRef.ListDependentPackets(tagname)


@traced_rpc_method("info")
def queue_suspend(queue_name):
    _scheduler.rpc_get_queue(queue_name).Suspend()


@traced_rpc_method("info")
def queue_resume(queue_name):
    _scheduler.rpc_get_queue(queue_name).Resume()


@readonly_method
@traced_rpc_method()
def queue_status(queue_name):
    q = _scheduler.rpc_get_queue(queue_name, create=False)
    return q.Status()


@readonly_method
@traced_rpc_method()
def queue_list(queue_name, filter, name_regex=None, prefix=None):
    name_regex = name_regex and re.compile(name_regex)
    q = _scheduler.rpc_get_queue(queue_name, create=False)
    return [pck.id for pck in q.rpc_list_packets(filter=filter, name_regex=name_regex, prefix=prefix)]


@readonly_method
@traced_rpc_method()
def queue_list_updated(queue_name, last_modified, filter=None):
    q = _scheduler.rpc_get_queue(queue_name, create=False)
    return [pck.id for pck in q.rpc_list_packets(last_modified=last_modified, filter=filter)]


@traced_rpc_method("info")
def queue_change_limit(queue_name, limit):
    _scheduler.rpc_get_queue(queue_name).ChangeWorkingLimit(limit)


@traced_rpc_method("info")
def queue_delete(queue_name):
    return _scheduler.rpc_delete_queue(queue_name)


@readonly_method
@traced_rpc_method()
def list_tags(name_regex=None, prefix=None, memory_only=True):
    name_regex = name_regex and re.compile(name_regex)
    return _scheduler.tagRef.ListTags(name_regex, prefix, memory_only)


@readonly_method
@traced_rpc_method()
def list_queues(name_regex=None, prefix=None, *args):
    name_regex = name_regex and re.compile(name_regex)
    return [(q.name, q.Status()) for q in _scheduler.qRef.itervalues()
            if (not name_regex or name_regex.match(q.name)) and \
               (not prefix or q.name.startswith(prefix))]


@readonly_method
@traced_rpc_method()
def pck_status(pck_id):
    pck = _scheduler.GetPacket(pck_id) or _scheduler.tempStorage.GetPacket(pck_id)
    if pck is not None:
        return pck.Status()
    raise MakeNonExistedPacketException(pck_id)


@traced_rpc_method("info")
def pck_suspend(pck_id, kill_jobs=False):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        return pck.rpc_suspend(kill_jobs)
    raise MakeNonExistedPacketException(pck_id)


@traced_rpc_method("info")
def pck_resume(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        return pck.rpc_resume()
    raise MakeNonExistedPacketException(pck_id)


@traced_rpc_method("info")
def pck_delete(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is None:
        raise MakeNonExistedPacketException(pck_id)
    return pck.rpc_remove()

@traced_rpc_method("info")
def pck_reset(pck_id, suspend=False, reset_tag=False, reset_message=None):
    pck = _scheduler.GetPacket(pck_id)
    if pck is None:
        raise MakeNonExistedPacketException(pck_id)

    if reset_tag:
        tag = pck.done_tag
        if tag:
            tag.Reset(reset_message)

    pck.rpc_reset(suspend=suspend)


@traced_rpc_method()
def check_binary_exist(checksum):
    return _scheduler.binStorage.HasBinary(checksum)


@traced_rpc_method("info")
def save_binary(bindata):
    _scheduler.binStorage.CreateFile(bindata.data)


@traced_rpc_method("info")
def check_binary_and_lock(checksum, localPath, tryLock=None):
    if tryLock is None:
        return _scheduler.binStorage.HasBinary(checksum) \
            or _scheduler.binStorage.CreateFileLocal(localPath, checksum)
    else:
        raise NotImplementedError('tryLock==True branch is not implemented yet!')


@traced_rpc_method()
def pck_add_binary(pck_id, binname, checksum):
    pck = _scheduler.tempStorage.GetPacket(pck_id) or _scheduler.GetPacket(pck_id)
    file = _scheduler.binStorage.GetFileByHash(checksum)
    if pck is not None and file is not None:
        pck.rpc_add_binary(binname, file)
        return
    raise MakeNonExistedPacketException(pck_id)


@traced_rpc_method()
def pck_add_resource(pck_id, name, path):
    pck = _scheduler.tempStorage.GetPacket(pck_id) or _scheduler.GetPacket(pck_id)
    if not pck:
        raise MakeNonExistedPacketException(pck_id)
    pck.rpc_add_resource(name, path)


@readonly_method
@traced_rpc_method()
def pck_list_files(pck_id):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        files = pck.rpc_list_files()
        return files
    raise MakeNonExistedPacketException(pck_id)


@readonly_method
@traced_rpc_method()
def pck_get_file(pck_id, filename):
    pck = _scheduler.GetPacket(pck_id)
    if pck is not None:
        file = pck.rpc_get_file(filename)
        return xmlrpclib.Binary(file)
    raise MakeNonExistedPacketException(pck_id)


@traced_rpc_method()
def queue_set_success_lifetime(queue_name, lifetime):
    q = _scheduler.rpc_get_queue(queue_name, create=False)
    q.SetSuccessLifeTime(lifetime)


@traced_rpc_method()
def queue_set_error_lifetime(queue_name, lifetime):
    q = _scheduler.rpc_get_queue(queue_name, create=False)
    q.SetErroredLifeTime(lifetime)


@traced_rpc_method("warning")
def set_backupable_state(bckpFlag, chldFlag=None):
    if bckpFlag is not None:
        if bckpFlag:
            _scheduler.ResumeBackups()
        else:
            _scheduler.SuspendBackups()
    if chldFlag is not None:
        if chldFlag:
            _scheduler.EnableBackupsInChild()
        else:
            _scheduler.DisableBackupsInChild()


@traced_rpc_method()
def get_backupable_state():
    return {"backup-flag": _scheduler.backupable, "child-flag": _scheduler.backupInChild}


@traced_rpc_method("warning")
def do_backup():
    return _scheduler.RollBackup(force=True, child_max_working_time=None)

class ApiServer(object):
    def __init__(self, port, poolsize, scheduler, allow_backup_method=False, readonly=False):
        self.scheduler = scheduler
        self.readonly = readonly
        self.allow_backup_method = allow_backup_method
        self.rpcserver = AsyncXMLRPCServer(poolsize, ("", port), AuthRequestHandler, allow_none=True)
        self.port = port
        self.rpcserver.register_multicall_functions()
        self.register_all_functions()

    def _non_readonly_func_stub(self, name):
        def stub(*args, **kwargs):
            raise NotImplementedError('Function %s is not available in readonly interface' % name)
        stub.__name__ = name
        return stub

    def register_function(self, func):
        if self.readonly:
            is_readonly_method = getattr(func, 'readonly_method', False)
            if not is_readonly_method:
                self.rpcserver.register_function(self._non_readonly_func_stub(func.__name__))
                return
        self.rpcserver.register_function(func)

    def register_all_functions(self):
        funcs = [
            check_binary_and_lock,
            check_binary_exist,
            check_tag,
            check_tags,
            create_packet,
            get_backupable_state,
            get_dependent_packets_for_tag,
            get_tag_local_state,
            list_cloud_tags_masks,
            list_queues,
            list_tags,
            lookup_tags,
            pck_add_binary,
            pck_add_resource,
            pck_add_job,
            pck_addto_queue,
            pck_delete,
            pck_get_file,
            pck_list_files,
            pck_moveto_queue,
            pck_reset,
            pck_resume,
            pck_status,
            pck_suspend,
            queue_change_limit,
            queue_delete,
            queue_list,
            queue_list_updated,
            queue_resume,
            queue_set_error_lifetime,
            queue_set_success_lifetime,
            queue_status,
            queue_suspend,
            reset_tag,
            save_binary,
            set_backupable_state,
            set_tag,
            unset_tag,
            update_tags,
        ]

        if self.allow_backup_method:
            funcs.append(do_backup)

        for func in funcs:
            self.register_function(func)

    def request_processor(self):
        rpc_fd = self.rpcserver.fileno()
        while self.alive:
            rout, _, _ = select.select((rpc_fd,), (), (), 0.01)
            if rpc_fd in rout:
                self.rpcserver.handle_request() # FIXME Can I use lower level API to not to hold threads?

    def start(self):
        self.xmlrpcworkers = [XMLRPCWorker(self.rpcserver.requests, self.rpcserver.process_request_thread)
                              for _ in xrange(self.rpcserver.poolsize)]
        self.alive = True
        self.main_thread = ProfiledThread(target=self.request_processor, name_prefix='Listen-%d' % self.port)
        for worker in self.xmlrpcworkers:
            worker.start()
        self.main_thread.start()

    def stop(self):
        self.alive = False
        for w in self.xmlrpcworkers:
            w.Kill()

def wait_cond_in_sleep(cond, deadline=None):
    while not (cond.is_set() or deadline and time.time() > deadline):
        time.sleep(1)
    return cond.is_set()

class RemDaemon(object):
    def __init__(self, scheduler, context):
        self._started = threading.Event()
        self._should_stop = threading.Event()
        self._stopped = threading.Event()

        self.scheduler = scheduler
        self.api_servers = [
            ApiServer(context.manager_port, context.xmlrpc_pool_size, scheduler,
                      allow_backup_method=context.allow_backup_rpc_method)
        ]
        if context.manager_readonly_port:
            self.api_servers.append(ApiServer(context.manager_readonly_port,
                                              context.readonly_xmlrpc_pool_size,
                                              scheduler,
                                              allow_backup_method=context.allow_backup_rpc_method,
                                              readonly=True))

        for srv in self.api_servers:
            srv.rpcserver.logRequests = False

        self.regWorkers = []
        self.timeWorker = None

        self._start()

    def _backups_loop(self):
        while True:
            nextBackupTime = time.time() + self.scheduler.backupPeriod

            logging.debug("rem-server\tnext backup time: %s" \
                % datetime.datetime.fromtimestamp(nextBackupTime).strftime('%H:%M'))

            # Don't sleep in 50ms in threading.py, accuracy here is not important
            if wait_cond_in_sleep(self._should_stop, deadline=nextBackupTime):
                return

            try:
                self.scheduler.RollBackup()
            except:
                pass

    def wait(self):
        wait_cond_in_sleep(self._should_stop) # To handle signals
        self._stopped.wait()

    def stop(self, wait=True):
        self._started.wait()

        self._should_stop.set()

        if wait:
            self._stopped.wait()

    def _run(self):
        self._should_stop.wait()
        self._stop()

        logging.debug("rem-server\tstart_final_backup")
        try:
            self.scheduler.RollBackup()
        except:
            logging.exception("final backup failed")

        logging.debug("rem-server\tstopped")
        self._stopped.set()

    def _stop(self):
        logging.debug("rem-server\tenter_stop")

        for server in self.api_servers:
            server.stop()
        logging.debug("rem-server\trpc_stopped")

        if self.timeWorker:
            self.timeWorker.Kill()
        logging.debug("rem-server\ttime_worker_stopped")

        self.scheduler.Stop1()
        logging.debug("rem-server\tafter_stop1")

        for worker in self.regWorkers:
            worker.Suspend()
        logging.debug("rem-server\tafter_suspend_workers")

        for worker in self.regWorkers:
            try:
                worker.Kill()
            except Exception:
                logging.exception("worker.Kill() failed")
        logging.debug("rem-server\tafter_kill_workers")

        for worker in self.regWorkers:
            worker.join()

        logging.debug("rem-server\tworkers_stopped")

    # TODO Make it nice
        self.scheduler.Stop2()
        logging.debug("rem-server\tjournal_stopped")

        logging.debug("rem-server\tbefore_backups_thread_join")
        self._backups_thread.join()
        logging.debug("rem-server\tafter_backups_thread_join")

        logging.debug("%s children founded after custom kill", len(multiprocessing.active_children()))
        for proc in multiprocessing.active_children():
            proc.terminate()

    # TODO Make it nice
        self.scheduler.Stop3()

    def _start_workers(self):
        self.scheduler.Start()
        logging.debug("rem-server\tafter_scheduler_start")

        self.regWorkers = [ThreadJobWorker(self.scheduler) for _ in xrange(self.scheduler.poolSize)]

        self.timeWorker = TimeTicker()
        self.timeWorker.AddCallbackListener(self.scheduler.schedWatcher)

        for worker in self.regWorkers + [self.timeWorker]:
            worker.start()
        logging.debug("rem-server\tafter_workers_start")

    def _start(self):
        self._start_workers()

        for server in self.api_servers:
            server.start()
        logging.debug("rem-server\tafter_rpc_workers_start")

        self._backups_thread = ProfiledThread(target=self._backups_loop, name_prefix="Backups")
        self._backups_thread.start()

        self._run_thread = ProfiledThread(target=self._run, name_prefix="Daemon")
        self._run_thread.start()

        logging.debug("rem-server\tall_started")

        self._started.set()


def scheduler_test(opts, ctx):
    global _context
    global _scheduler

    _context = ctx
    _scheduler = create_scheduler(ctx)

    def tag_listeners_stats(tagRef):
        tag_listeners = {}
        for tag in tagRef.inmem_items.itervalues():
            listenCnt = tag.GetListenersNumber()
            tag_listeners[listenCnt] = tag_listeners.get(listenCnt, 0) + 1
        return tag_listeners

    def print_tags(sc):
        for tagname, tagvalue in sc.tagRef.ListTags():
            if tagvalue: print "tag: [{0}]".format(tagname)

    print_tags(_scheduler)
    qname = "userdata"
    for q_stat in list_queues():
        print q_stat
    pendingLength = workedLength = suspendLength = 0
    if qname in _scheduler.qRef:
        pendingLength = len(_scheduler.qRef[qname].pending)
        workedLength = len(_scheduler.qRef[qname].worked)
        suspendLength = len(_scheduler.qRef[qname].suspended)
        for pck_id in queue_list(qname, "waiting"):
            pck_suspend(pck_id)
            pck_resume(pck_id)
    print "tags listeners statistics: %s" % tag_listeners_stats(_scheduler.tagRef)

    #serialize all data to data.bin file
    stTime = time.time()
    _scheduler.SaveBackup("data.bin")
    print "serialize time: %.3f" % (time.time() - stTime)

    #print memory usage statistics
    try:
        import guppy

        mem = guppy.hpy()
        print mem.heap()
    except:
        logging.exception("guppy error")
    #deserialize backward attempt
    stTime = time.time()
    tmpContext = Context(opts.config)
    sc = Scheduler(tmpContext)
    sc.LoadBackup("data.bin")
    if qname in sc.qRef:
        print "PENDING: %s => %s" % (pendingLength, len(sc.qRef[qname].pending))
        print "WORKED: %s => %s" % (workedLength, len(sc.qRef[qname].worked))
        print "SUSPEND: %s => %s" % (suspendLength, len(sc.qRef[qname].suspended))
        print "deserialize time: %.3f" % (time.time() - stTime)
    print "tags listeners statistics: %s" % tag_listeners_stats(sc.tagRef)
    print "scheduled tasks: ", sc.schedWatcher.tasks.qsize(), sc.schedWatcher.workingQueue.qsize()
    while not sc.schedWatcher.tasks.empty():
        runner, runtm = sc.schedWatcher.tasks.get()
        print runtm, runner


def _init_fork_locking(ctx):
    set_timeout = getattr(rem.fork_locking, 'set_fork_friendly_acquire_timeout', None)

    if not set_timeout:
        return

    set_timeout(ctx.backup_fork_lock_friendly_timeout)


def start_daemon(ctx, sched, wait=True):
    global _context
    global _scheduler

    _context = ctx
    _scheduler = sched

    _init_fork_locking(ctx)

    should_stop = [False]

    def _log_signal(sig):
        logging.warning("rem-server\tsignal %s has gotten", sig)

    def set_handler(handler):
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(sig, handler)

    def signal_handler0(sig, frame):
        _log_signal(sig)
        should_stop[0] = True

    set_handler(signal_handler0)

    daemon = RemDaemon(sched, ctx)

    def signal_handler1(sig, frame):
        _log_signal(sig)
        daemon.stop(wait=False)

    set_handler(signal_handler1)

    if should_stop[0]:
        daemon.stop(wait=False)

    def join():
        daemon.wait()
        set_handler(signal.SIG_DFL)

    return daemon, join


def parse_arguments():
    p = argparse.ArgumentParser()

    p.add_argument('-c', '--config', dest='config', default='rem.cfg')
    p.add_argument('--yt-writer-count', dest='yt_writer_count', type=int, default=20)
    p.add_argument('mode', nargs='?', default='start')

    return p.parse_args()


def create_scheduler(ctx, restorer=None):
    sched = Scheduler(ctx)
    sched.Restore(restorer=restorer)
    return sched


def run_server(ctx):
    osspec.set_process_title("[remd]%s" % ((" at " + ctx.network_name) if ctx.network_name else ""))

    def logged(f, *args):
        logging.debug("rem-server\tbefore_%s" % f.__name__)
        return f(*args)

    sched = logged(
        create_scheduler, ctx)

    logged(
        sched.cleanup_bin_storage_fs)

    #logged(
        #sched.cleanup_packet_storage_fs)

    start_daemon(ctx, sched)[1]()


def init_logging(ctx):
    rem_logging.reinit_logger(ctx)


def create_process_runners(ctx):
    pgrpguard_binary = ctx.pgrpguard_binary

    runner = None

    if ctx.subprocsrv_runner_count:
        runner = subprocsrv.create_runner(
            pool_size=ctx.subprocsrv_runner_count,
            pgrpguard_binary=ctx.pgrpguard_binary
        )

    ctx._subprocsrv_runner = runner

    ctx.run_job = rem.job.create_job_runner(runner, ctx.pgrpguard_binary)

    def create_aux_runner():
        ordinal_runner = rem.subprocsrv_fallback.Runner()

        return rem.subprocsrv_fallback.RunnerWithFallback(runner, ordinal_runner) \
            if runner \
            else ordinal_runner

    ctx.aux_runner = create_aux_runner()


def _init_sandbox(ctx):
    ctx.sandbox = rem.sandbox.Client(ctx.sandbox_api_url, ctx.sandbox_api_token, timeout=15.0)
    ctx.sandbox_subproc = subprocsrv.create_runner()

# TODO
    shr = rem.resource_sharing.Sharer(
        subproc=ctx.sandbox_subproc,
        sandbox=ctx.sandbox,
        task_owner=ctx.sandbox_task_owner,
        task_priority=(
            rem.sandbox.TaskPriority.Class.SERVICE,
            rem.sandbox.TaskPriority.SubClass.NORMAL
        ),
    )

    ctx.sbx_resource_sharer = shr

    shr.start()


def _copy_executor_files(dir):
    code_root = os.path.dirname(sys.modules[__name__].__file__)

    if code_root == '':
        code_root = '.'

    shutil.copy(code_root + '/sbx_run_packet.py', dir + '/')

    shutil.copytree(
        code_root + '/rem',
        dir + '/rem',
        ignore=(lambda _, files: [f for f in files if not f.endswith('.py')]),
    )


def _share_sandbox_executor(ctx):
    with NamedTemporaryDir(prefix='rem_sbx_exe') as dir:
        os.chmod(dir, 0775)
        _copy_executor_files(dir)

        archive_basename = 'rem_executor.tar'
        archive_filename = dir + '/' + archive_basename

        subprocess.check_call(
            ['tar', '-C', dir, '-cf', archive_filename, 'sbx_run_packet.py', 'rem'])

        id = ctx.sbx_resource_sharer.share(
            'REM_JOBPACKET_EXECUTOR',
            description='%s @ %s' % (
                os.uname()[1],
                datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S')
            ),

            #name='executor',
            #filename=dir,
            #is_root=True,
            name=archive_basename,
            filename=archive_filename,

            arch='linux',
            ttl=10 * 86400, # TODO
        )

        return id.get()


def init(ctx):
    init_logging(ctx)
    create_process_runners(ctx)
    rem.common.set_proc_runner(ctx.aux_runner)

    delayed_executor.start()

    if ctx.sandbox_api_url:
        _init_sandbox(ctx)
        ctx.sandbox_executor_resource_id = _share_sandbox_executor(ctx)


def main():
    opts = parse_arguments()

    ctx = Context(opts.config)

    if opts.mode == 'test':
        ctx.log_warn_level = 'debug'
        ctx.log_to_stderr = True
        ctx.register_objects_creation = True

    init(ctx)

    if opts.mode == "start":
        run_server(ctx)

    elif opts.mode == "convert-on-disk-tags":
        ctx.fix_bin_links_at_startup = False
        Scheduler.convert_on_disk_tags_to_cloud(ctx, yt_writer_count=opts.yt_writer_count)

    elif opts.mode == "test":
        scheduler_test(opts, ctx)

    else:
        raise RuntimeError("Unknown exec mode '%s'" % opts.mode)

    if ctx._subprocsrv_runner:
        ctx._subprocsrv_runner.stop()
    delayed_executor.stop()

    logging.debug("rem-server\texit_main")


if __name__ == "__main__":
    main()
