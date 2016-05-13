# -*- coding: utf-8 -*-
import os
import sys
import time
import base64
import cPickle
from rem.xmlrpc import SimpleXMLRPCServer, ServerProxy as XMLRPCServerProxy
import threading
from Queue import Queue as ThreadSafeQueue

from rem.profile import ProfiledThread
import sandbox_packet
import rem.sandbox as sandbox
from rem_logging import logger as logging
from rem.future import Promise
from packet_state import PacketState
from job_graph import GraphState
from rem.common import PickableLock

remote_packets_dispatcher = None

###########################
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

def wrap_string(s, width):
    return '\n'.join(
        s[i * width : (i + 1) * width]
            for i in xrange((len(s) - 1) / width + 1)
    )


class ActionQueue(object):
    __STOP_INDICATOR = object()

    def __init__(self, thread_count=1, thread_name_prefix='Thread'):
        self._queue = ThreadSafeQueue()
        self._thread_name_prefix = thread_name_prefix
        self._thread_count = thread_count
        self.__start()

    def __start(self):
        self._threads = [
            ProfiledThread(target=self.__worker, name_prefix=self._thread_name_prefix)
                for _ in xrange(self._thread_count)
        ]

        for t in self._threads:
            t.start()

    def stop(self):
        for _ in self._threads:
            self._queue.put(self.__STOP_INDICATOR)

        for t in self._threads:
            t.join()

    def invoke(self, task):
        self._queue.put(task)

    def __worker(self):
        while True:
            task = self._queue.get()

            if task is self.__STOP_INDICATOR:
                break

            try:
                task()
            except Exception:
                logging.exception("")


class TaskStateGroups(object):
    DRAFT = 1
    ACTIVE = 2
    TERMINATED = 3
    ANY = 4


class SandboxTaskStateAwaiter(object):
    DEFAULT_UPDATE_INTERVAL = 5.0

    def __init__(self, sandbox, on_status_change, update_interval=DEFAULT_UPDATE_INTERVAL):
        self._sandbox = sandbox
        self._on_status_change = on_status_change
        self._next_job_id = 1
        self._should_stop = False
        self._lock = threading.Lock()
        self._something_happend = threading.Condition(self._lock)
        self._worker_thread = None
        self._update_interval = update_interval

        self._start()

    def _start(self):
        self._worker_thread = ProfiledThread(target=self._loop, name_prefix='SbxStateMon')
        self._worker_thread.start()

    def stop(self):
        with self._lock:
            self._should_stop = True
            self._something_happend.notify()

        self._main_thread.join()

    def await(self, task_id, status_group):
        with self._lock:
            job_id = self._next_job_id
            self._next_job_id += 1

            #was_empty = not self._incoming

            self._incoming[task_id] = (job_id, status_group)

            #if was_empty:
                #self._something_happend.notify()

        return job_id

    def _loop(self):
        running = {}
        next_update_time = time.time() + self._update_interval

        while True:
            with self._lock:
                if not self._should_stop:
                    now = time.time()
                    if now < next_update_time:
                        self._something_happend.wait(next_update_time - now)

                if self._should_stop:
                    return

                new_jobs, self._incoming = self._incoming, {}

            if new_jobs:
                for task_id, (job_id, status_group) in new_jobs:
                    running[task_id] = (job_id, status_group)

            try:
                statuses = self._sandbox.list_task_statuses(running.keys())

            except (sandbox.NetworkError, sandbox.ServerInternalError) as e:
                pass

            except Exception as e:
                logging.exception("Can't fetch task statuses from Sandbox")

            else:
                for task_id, status in statuses:
                    job_id, status_group = running[task_id]

                    if self._in_status_group(status, status_group):
                        running.pop(task_id)
                        self._on_status_change(task_id, job_id, status_group)

            next_update_time = time.time() + self._update_interval

    @staticmethod
    def _in_status_group(self, status, group):
        if group == TaskStateGroups.ANY:
            return True

        if status == 'DRAFT':
            return group == TaskStateGroups.DRAFT

        elif status in ['SUCCESS', 'RELEASING', 'RELEASED', 'FAILURE', 'DELETING',
                        'DELETED', 'NO_RES', 'EXCEPTION', 'TIMEOUT']:
            return group == TaskStateGroups.TERMINATED

        else:
            return group == TaskStateGroups.ACTIVE


class RemotePacketsDispatcher(object):
    _SANDBOX_TASK_CREATION_RETRY_INTERVAL = 10.0

    def __init__(self):
        #self.listen_port = listen_port
        #self.by_pck_id = {}
        self.by_task_id = {}
# TODO
        self._sbx_task_kill_timeout = 14 * 86400 # TODO
        self._sbx_task_owner = 'guest'
        self._sbx_task_priority = (
            sandbox.TaskPriority.Class.SERVICE,
            sandbox.TaskPriority.SubClass.NORMAL)

    def _create_rpc_server(self):
        srv = SimpleXMLRPCServer(self._rpc_listen_addr)
        #srv.register_function(self._on_rpc_ping, 'ping') # FIXME Don't remember what for
        srv.register_function(self._on_rpc_update_graph, 'update_graph')
        #srv.register_function(self._on_rpc_mark_as_finished, 'mark_as_finished')
        return srv

        #def start(self, io_invoker, sandbox):
    def start(self, ctx):
        self._executor_resource_id = ctx.sandbox_executor_resource_id
        self._rpc_listen_addr = ('ws30-511.yandex.ru', 8000) # TODO XXX
        self._rpc_server = self._create_rpc_server()
        ProfiledThread(target=self._rpc_server.serve_forever, name_prefix='SbxRpc').start()

        # Must be shared with all outer sanbox api users
        self._sbx_invoker = ActionQueue(thread_count=10, thread_name_prefix='SbxIO')

        self._rpc_invoker = ActionQueue(thread_count=10, thread_name_prefix='RpcIO')

        self._sandbox = sandbox.Client(ctx.sandbox_api_url, ctx.sandbox_api_token, timeout=15.0)

        self._status_awaiter = SandboxTaskStateAwaiter(self._sandbox, self._on_task_status_change)

    def stop(self):
        raise NotImplementedError()

# TODO max_restarts=0
# TODO kill_timeout=14 * 86400
# FIXME fail_on_any_error=False XXX А на каких он не падает?
# FIXME С одной стороны хочется hidden, но тогда Рома не сможет позырить

# XXX
# 1. Может ли таск порестартоваться даже при max_restarts=0

    # XXX TODO
    # We must check task statuses periodically ourselves
    # for packets that dont' communicate with us for a 'long time'

    # XXX TODO
    # Send instance_id in requests to instance as assert

    # XXX TODO
    # Instance must also ping server (if server doesn't ping instance)
    # so REM-server will register instance's remote_addr after
    # loading old backup (after server's fail)

    def register_packet(self, pck):
        self._start_create_sandbox_task(pck)

# XXX FIXME We can't identify packet by pck_id in async/delayed calls
#           because packet may be recreated by PacketBase
#           (or we must cancel invokers (not only delayed_executor))

    def _start_create_sandbox_task(self, pck):
        self._sbx_invoker.invoke(lambda : self._do_create_sandbox_task(pck))

    def _start_start_sandbox_task(self, pck):
        self._sbx_invoker.invoke(lambda : self._do_start_sandbox_task(pck))

    #def _start_delete_task(self, task_id):
        #self._sbx_invoker.invoke(lambda : self._sandbox.delete_task(task_id)) # no retries

    def _sbx_create_task(self, pck):
        return self._sandbox.create_task(
            'RUN_REM_JOBPACKET',
            {
                'executor_resource': self._executor_resource_id,
                'snapshot_data': wrap_string(pck._start_snapshot_data, 79) \
                    if pck._start_snapshot_data \
                    else None,
                'snapshot_resource': pck._start_snapshot_resource_id,
                'custom_resources': pck._custom_resources,
                'rem_server_addr': ('%s:%d' % self._rpc_listen_addr),
                #'instance_id': instance_id,
            }
        )

    def _sbx_update_task(self, pck, task):
        task.update(
            max_restarts=0,
            kill_timeout=self._sbx_task_kill_timeout,
            owner=self._sbx_task_owner,
            priority=self._sbx_task_priority,
            notifications=[],
            description='%s @ %s\n%s\n%s' % (
                None, self._rpc_listen_addr, '_pck_name_TODO_', '_jobs_graph_TODO_')
            #fail_on_any_error=True, # FIXME What?
        )

    def _do_create_sandbox_task(self, pck):
        def reschedule_if_need():
            with pck.lock:
                if pck._target_stop_mode:
                    return

                self._schedule(
                    pck,
                    self._start_create_sandbox_task,
                    timeout=self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)

        with pck.lock:
            if pck._target_stop_mode:
                return

        def handle_unknown_error(e):
            with pck.lock:
                if pck._target_stop_mode:
                    return

                pck._state = TaskState.TERMINATED
                self._exception = e
                self._on_end_of_life()

        try:
            task = self._sbx_create_task(pck)
        except (sandbox.NetworkError, sandbox.ServerInternalError) as e:
            reschedule_if_need()
            return
        except Exception as e:
            logging.exception('')
            handle_unknown_error(e)
            return

        logging.debug('sbx:%d for %s created' % (task.id, pck.id))

        with pck.lock:
            if pck._target_stop_mode:
                return

        try:
            self._sbx_update_task(pck, task)

        except (sandbox.NetworkError, sandbox.ServerInternalError) as e:
            reschedule_if_need()
            #self._start_delete_task(task.id)
            return
        except Exception as e:
            logging.exception('')
            handle_unknown_error(e)
            #self._start_delete_task(task.id)
            return

        with pck.lock:
            if pck._target_stop_mode:
                return

            # FIXME fork locking (mallformed pck state)
            #pck._instance_id = instance_id
            pck._sandbox_task_id = task.id
            pck._state = TaskState.STARTING

            self.by_task_id[task.id] = pck

        self._do_start_sandbox_task(pck)

    def _do_start_sandbox_task(self, pck):
        try:
            self._sandbox.start_task(pck._sandbox_task_id)

        # Possible events before lock will be acquired in this func:
        # - not final GRAPH_UPDATE
        # - final GRAPH_UPDATE
        # - STOP_GRACEFULLY/STOP/CANCEL

        # XXX No task status changes may appear here, because awaiting is not racy
        # - final GRAPH_UPDATE + task terminated
        # - task terminated wo GRAPH_UPDATE's

        except Exception as e:
            with pck.lock:
                if pck._state != TaskState.STARTING:
                    return

                # TODO Don't forget to take into account ._target_stop_mode in _on_task_status_change

                # Here we don't know if task is really started
                self._is_start_error_permanent = \
                    isinstance(e, (sandbox.NetworkError, sandbox.ServerInternalError))

                self._state = TaskState.CHECKING_START_ERROR
                self._start_error = e

                self._wait_task_state(pck, TaskStateGroups.ANY)

            return

        with pck.lock:
            if pck._state != TaskState.STARTING:
                return

            pck._state = TaskState.STARTED

            self._wait_task_state(pck, TaskStateGroups.TERMINATED)

            assert not self._peer_addr
            #if self._target_stop_mode:
            #    <waiting for peer addr>
            #
            #    We can't STOP* for now, because we have no _peer_addr
            #    We theoretically can do CANCEL using Sandbox API, but
            #    I prefer do all STOP*/CANCEL in the same way.

            # XXX FIXME If firewall has no holes for us,
            # we can't do STOP*, but theoretically can do CANCEL

    def _wait_task_state(self, pck, status_group):
        pck._status_await_job_id = self._status_awaiter.await(pck._sandbox_task_id, status_group)

# The hard way
    # must be called under lock
    #def _schedule(self, pck, impl, timeout=None):
        #id = None
        #wrap = lambda : self._execute_scheduled(pck, id, impl)
        #id = id(wrap)
        #cancel = delayed_executor.schedule(wrap, timeout=timeout)

        #pck._sched = (id, cancel)

    #def _execute_scheduled(self, pck, id, impl):
        #with pck.lock:
            #if not pck._sched or pck._sched[0] != id:
                #return

            #pck._sched = None

            #impl()

    # must be called under lock
    def _schedule(self, pck, impl, timeout=None):
        wrap = lambda id: self._execute_scheduled(pck, id, impl)
        pck._sched = delayed_executor.schedule(wrap, timeout=timeout)

    def _execute_scheduled(self, pck, id, impl):
        with pck.lock:
            if not pck._sched:
                if pck._does_reach_end_of_life():
                    pck._on_end_of_life()
                return

            elif pck._sched.id != id:
                return

            pck._sched = None

        # ._sched is None means that impl is running
        # When impl stop it will set ._sched again or modify other fields of pck
        impl(pck)

    #def _on_graph_update(self, peer_addr, task_id, jobs, finished_status):
    def _on_rpc_update_graph(self, task_id, peer_addr, state, is_final):
        pck = self.by_task_id.get(task_id)

        if not pck:
            raise NonExistentInstanceError()

        with pck.lock:
            assert pck._state not in [
                TaskState.CREATING,
                TaskState.TERMINATED,
                TaskState.FETCHING_RESOURCE_LIST,
                TaskState.FETCHING_RESULT_RESOURCE
            ]

            if pck._state in [TaskState.STARTING, TaskState.CHECKING_START_ERROR]:
                pck._state = TaskState.STARTED

            if pck._sched:
                pck._sched()
                pck._sched = None

            if not pck._peer_addr:
                pck._peer_addr = peer_addr

                if pck._target_stop_mode and not is_final:
                    pck._start_packet_stop(pck)

            if pck._target_stop_mode != StopMode.CANCEL:
                pck._update_graph(jobs, finished_status)

    def _on_task_status_change(self, task_id, job_id, status_group):
        #with self.lock:
        if True:
            pck = self.by_task_id.get(task_id)

        if not pck:
            return

        with pck.lock:
            if pck._status_await_job_id != job_id:
                return

            pck._status_await_job_id = None

            state = self._state

            assert state not in [
                TaskState.CREATING,
                TaskState.TERMINATED,
                TaskState.FETCHING_RESOURCE_LIST,
                TaskState.FETCHING_RESULT_RESOURCE
            ]

    # TODO XXX
    # Пока в черне обрисовал

            if state == TaskState.STARTING:
                if status_group == TaskStateGroups.DRAFT:
                    raise AssertionError()

                elif status_group == TaskStateGroups.ACTIVE:
                    self._state = TaskState.STARTED

                elif status_group == TaskStateGroups.TERMINATED:
                    self._state = TaskState.FETCHING_RESOURCE_LIST #1
                    self._start_fetch_resource_list(pck)

            elif state == TaskState.CHECKING_START_ERROR:
                # TODO drop_sched

                if status_group == TaskStateGroups.DRAFT:
                    if self._is_start_error_permanent:
                        self._state = TaskState.TERMINATED # with error
                        # TODO _on_end_of_life
                    else:
                        self._state = TaskState.STARTING
                        self._start_start_sandbox_task(pck)

                elif status_group == TaskStateGroups.ACTIVE:
                    self._state = TaskState.STARTED

                elif status_group == TaskStateGroups.TERMINATED:
                    self._state = TaskState.FETCHING_RESOURCE_LIST #2
                    self._start_fetch_resource_list(pck)

            elif state == TaskState.STARTED:
                if status_group == TaskStateGroups.DRAFT:
                    raise AssertionError()

                elif status_group == TaskStateGroups.ACTIVE:
                    pass

                elif status_group == TaskStateGroups.TERMINATED:
                    if not pck._has_final_update():
                        self._state = TaskState.FETCHING_RESOURCE_LIST #1
                        self._start_fetch_resource_list(pck)
                    else:
                        self._state = TaskState.TERMINATED # with error
                        # TODO _on_end_of_life

    def stop_packet(self, pck, kill_jobs):
        self._stop_packet(pck, StopMode.STOP if kill_jobs else StopMode.STOP_GRACEFULLY)

    def cancel_packet(self, pck):
        self._stop_packet(pck, StopMode.CANCEL)

    def _stop_packet(self, pck, stop_mode):
        with pck.lock:
            if pck._target_stop_mode >= stop_mode:
                return

            def drop_sched():
                if pck._sched:
                    pck._sched()
                    pck._sched = None

            def really_cancel():
                drop_sched()
                pck._state = TaskState.TERMINATED
                pck._on_end_of_life()

            pck._target_stop_mode = stop_mode

            state = pck._state

            if state in [TaskState.CREATING, TaskState.FETCHING_RESOURCE_LIST, TaskState.FETCHING_RESULT_RESOURCE] \
                or state == TaskState.STARTING and pck._sched:

                really_cancel()

            elif state in [TaskState.STARTING, TaskState.CHECKING_START_ERROR]:
                pass # later

            # FIXME
            elif state == TaskState.TERMINATED:
                raise AlreadyTerminated()

            elif state == TaskState.STARTED:
                if pck._peer_addr:
                    self._start_packet_stop(pck)

            else:
                raise Unreachable()

    def _do_stop_packet(self, pck):
        task_id = pck._sandbox_task_id
        stop_mode = pck._target_stop_mode

        proxy = rem.xmlrpc.ServerProxy(url=..., timeout=...)

        try:
            if stop_mode == StopMode.CANCEL:
                proxy.cancel(task_id)
            else:
                proxy.stop(task_id, kill_jobs=stop_mode == StopMode.STOP)

        except (...ECONNREFUSED, WrongTaskId): # TODO
            return

        except Exception as e:
            logging.exception("Failed to send stop to packet") # XXX For devel
        # TODO ReComment
            #logging.warning("Failed to send stop to packet: %s" % e)

            with pck.lock:
                if pck._state != TaskState.STARTED:
                    return

                assert not self._sched

                self._start_packet_stop(pck)

        else:
            with pck.lock:
                assert pck._state == TaskState.STARTED # FIXME XXX

                if self._sent_stop_mode < stop_mode:
                    self._sent_stop_mode = stop_mode

    def _start_packet_stop(self, pck):
        self._rpc_invoker.invoke(lambda : self._do_stop_packet(pck))


#CHECKING_START_PERM_ERROR / CHECKING_START_TEMP_ERROR
    #._state = CHECKING_START_ERROR
    #._is_start_error_permanent = bool

#CREATING
    #._state = CREATING

#CREATION_SCHEDULED
    #._state = CREATING
    #._sched = function

#CREATING_AND_CANCELLED
    #._state = CREATING
    #._sched = None
    #._target_stop_mode = StopMode.CANCEL

#EXECUTING_HAS_PEER_ADDR
    #._peer_addr = not None
    #._state = CREATED

#GOT_RESULT_TASK_WAIT
    #._result = not None (set from rpc)
    #._state = STARTED

#TASK_WAIT_AND_CANCELLED
    #._result = not None (set from rpc)
    #._state = STARTED
    #._target_stop_mode = StopMode.CANCEL

#STARTING
    #._state = STARTING
    #._sched = None

#START_SCHEDULED
    #._state = STARTING
    #._sched = function

#FETCHING_TERMINATED_TASK_INFO
    #._state = FETCHING_RESOURCE_LIST
    #._target_stop_mode = None
    #._sched = None

#FETCHING_TERMINATED_TASK_INFO_AND_CANCELLED
    #._state = FETCHING_RESOURCE_LIST
    #._target_stop_mode = StopMode.CANCEL

#EXECUTING_AND_STOPPING
    #._state = STARTED
    #._target_stop_mode != StopMode.NONE
    #._sched = None

#EXECUTING_AND_STOPPING_SCHEDULED
    #._state = STARTED
    #._target_stop_mode != StopMode.NONE
    #._sched = function

#EXECUTING_AND_STOP_SENT
    #._state = STARTED
    #._target_stop_mode != StopMode.NONE
    #._target_stop_mode == ._sent_stop_mode
    #._sched = is None

#TERMINATED_CANCELLED
    #._state = TERMINATED
    #._target_stop_mode != StopMode.NONE

#._target_stop_mode = 
#._sent_stop_mode
#._result
#._suspended_state_resource_id
#._peer_addr
#._state

# XXX
# Throw on any event in TERMINATED state
# XXX


class StopMode(object):
    NONE = 0
    STOP_GRACEFULLY = 1
    STOP = 2
    CANCEL = 3


# Not task state actually
class TaskState(object):
    CREATING = 1
    STARTING = 3
    CHECKING_START_ERROR = 4 # permanent or temporary
    STARTED = 6
    TERMINATED = 7
    FETCHING_RESOURCE_LIST = 8
    FETCHING_RESULT_RESOURCE = 9

class AlreadyTerminated(RuntimeError):
    pass

class Unreachable(AssertionError)
    pass


class SandboxRemotePacket(object):
    def __init__(self, ops, pck_id, snapshot_data, snapshot_resource_id, custom_resources):
        self.id = pck_id
        self._ops = ops
        #self.lock = ops._ops.lock # XXX TODO
        #self.lock = threading.Lock()
        self.lock = PickableLock()
        self._target_stop_mode = StopMode.NONE
        self._sent_stop_mode   = StopMode.NONE # at least helpfull for backup loading
        self._state = TaskState.CREATING
        #self._instance_id = None
        self._start_snapshot_resource_id = snapshot_resource_id
        self._start_snapshot_data = snapshot_data
        self._custom_resources = custom_resources
        self._sandbox_task_id = None
        self._sched = None
        self._peer_addr = None

        remote_packets_dispatcher.register_packet(self)

    def cancel(self):
        remote_packets_dispatcher.cancel_packet(self)

    #def restart(self): # TODO .fast_restart
        #remote_packets_dispatcher.restart_packet(self)

    def stop(self, kill_jobs):
        remote_packets_dispatcher.stop_packet(self, kill_jobs)

    def get_result(self):
        raise NotImplementedError()

    def _on_update(self, history, jobs):
        assert not self._cancelled
        self._ops._on_sandbox_packet_update(history, jobs)

    def _on_end_of_life(self):
        self._ops._on_packet_terminated()


def _produce_snapshot_data(pck_id, graph):
    pck = sandbox_packet.Packet(pck_id, graph)
    return base64.b64encode(cPickle.dumps(pck, 2))


class SandboxJobGraphExecutorProxy(object):
    def __init__(self, ops, pck_id, graph, custom_resources):
        self._ops = ops
        self.lock = self._ops.lock
        self.pck_id = pck_id
        self._graph = graph
        self._custom_resources = custom_resources

        self._remote_packet = None
        self._snapshot_resource_id = None
        self._result_resource_id = None # TODO FIXME

        self.do_not_run = False # FIXME
        self.cancelled = False # FIXME
        self.time_wait_deadline = None
        self.time_wait_sched = None
        self.result = None

    # TODO FIXME
        self.remote_history = [] # TODO
        self.detailed_status = {}
        self.state = None
        #self.state = ReprState.PENDING
        #self.history = [] # XXX TODO Непонятно вообще как с этим быть

    def init(self):
        self._update_state()

    def _create_remote_packet(self):
        return SandboxRemotePacket(
            self,
            self.pck_id,
            snapshot_data=_produce_snapshot_data(self.pck_id, self._graph) \
                if not self._snapshot_resource_id else None,
            snapshot_resource_id=self._snapshot_resource_id,
            custom_resources=self._custom_resources
        )

    #def get_state(self):
        #if not self.history:
            #return ReprState.PENDING
        #return self.history[-1][0]

    def produce_detailed_status(self):
        return self.detailed_status

    def is_stopping(self):
        with self.lock:
            return (self.do_not_run or self.cancelled) and self._remote_packet
            # (WORKING | CANCELLED) || (WORKING | SUSPENDED)

########### Ops for _remote_packet
    def _on_sandbox_packet_update(self, history, jobs_statuses):
        with self.lock:
            if self.cancelled:
                return

            self.remote_history = history # TODO
            self.detailed_status = jobs_statuses

    # on sandbox task stopped + resources list fetched
    def _on_packet_terminated(self, how):
        self._do_on_packet_terminated(how)
        self._update_state()

    def _do_on_packet_terminated(self):
        def on_stop():
            self._remote_packet = None
            self._stop_promise.set()
            self._stop_promise = None
            self._ops.on_job_graph_becomes_null()

        with self.lock:
            res = self._remote_packet.get_result()

            if self.cancelled:
                self.cancelled = False
                self.remote_history = []
                self.detailed_status = {}
                on_stop()
                return

            if res.exceptioned: # TODO Rename
                logging.warning('...')

                # XXX TODO XXX Rollback history/Status to prev state

                if self.do_not_run:
                    on_stop()
                else:
                    self._remote_packet = self._create_remote_packet()

                return

            # Even on .do_not_run -- ERROR/SUCCESSFULL more prioritized
            # (TODO Support this rule in PacketBase)

            if res.status == GraphState.TIME_WAIT:
                self.time_wait_deadline = res.time_wait_deadline
                self.time_wait_sched = \
                    delayed_executor.schedule(self._stop_time_wait,             # TODO Fix races
                                              deadline=self.time_wait_deadline)

            elif res.status == GraphState.SUCCESSFULL:
                self.result = True
                self._result_resource_id = res.result_resource_id

            elif res.status == GraphState.ERROR:
                self.result = False

            self._result_snapshot_resource_id = res.snapshot_id \
                if res.status != GraphState.SUCCESSFULL else None

            on_stop()

    def _stop_time_wait(self):
        with self.lock:
            # TODO Fix races
            self.time_wait_deadline = None
            self.time_wait_sched = None

            if not self.do_not_run:
                self._remote_packet = self._create_remote_packet()

            self._update_state()

    def _update_state(self):
        new = self._calc_state()
        if new == self.state:
            return

        self.state = new
        self._ops.on_state_change()

    def _calc_state(self):
        with self.lock:
            if self._remote_packet:
                state = GraphState.WORKING

                if self.cancelled:
                    state |= GraphState.CANCELLED
                elif self.do_not_run:
                    state |= GraphState.SUSPENDED

                return state

            elif self.result is not None:
                return GraphState.SUCCESSFULL if self.result else GraphState.ERROR

            elif self.time_wait_deadline:
                return GraphState.TIME_WAIT

            elif self.do_not_run:
                return GraphState.SUSPENDED

            else:
                return GraphState.PENDING_JOBS

    def stop(self, kill_jobs):
        with self.lock:
            if not self._remote_packet:
                raise
            if self.cancelled:
                return # FIXME

            self.do_not_run = True
            self._remote_packet.stop(kill_jobs)
            self._update_state()

    def start(self):
        with self.lock:
            if self._remote_packet:
                raise RuntimeError()

            self.do_not_run = False

            self._stop_promise = Promise()
            self._remote_packet = self._create_remote_packet()

            self._update_state()

            return self._stop_promise.to_future()

    def cancel(self):
        with self.lock:
            if not self._remote_packet:
                return

            self.do_not_run = True
            self.cancelled = True
            self._result_snapshot_resource_id = None

            self._remote_packet.cancel()
            self._update_state()

    def is_null(self):
        return self._remote_packet is None

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
    #def __init__(self, pck):
        #self._pck = pck
        #self.lock = pck.lock


#class _SandboxPacketJobGraphExecutorProxyOps(object):
    def __init__(self, pck):
        self.pck = pck
        self.lock = pck.lock

    def on_state_change(self):
        self.pck._on_graph_executor_state_change()

    def set_successfull_state(self):
        self.pck._change_state(PacketState.SUCCESSFULL)

    def set_errored_state(self):
        self.pck._change_state(PacketState.ERROR)

    def job_done_successfully(self, job_id):
        tag = self.pck.job_done_tag.get(job_id)
        if tag:
            tag.Set()

    def create_job_runner(self, job):
        raise AssertionError()

    def on_job_graph_becomes_null(self):
        self.pck._on_job_graph_becomes_null()


