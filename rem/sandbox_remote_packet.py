# -*- coding: utf-8 -*-
import os
import sys
import time
import base64
import cPickle as pickle
from rem.xmlrpc import SimpleXMLRPCServer, ServerProxy as XMLRPCServerProxy
import threading
from Queue import Queue as ThreadSafeQueue
import errno
import socket
import xmlrpclib

from rem.profile import ProfiledThread
import sandbox_packet
import rem.sandbox as sandbox
from rem.sandbox_tasks import TaskStateGroups, SandboxTaskStateAwaiter
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

class WrongTaskIdError(RuntimeError):
    pass


def is_xmlrpc_exception(exc, type):
    return isinstance(exc, xmlrpclib.Fault) and type.__name__ in exc.faultString


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
        return srv

    def start(self, ctx):
        self._executor_resource_id = ctx.sandbox_executor_resource_id
        self._rpc_listen_addr = ('ws30-511.search.yandex.net', 8000) # TODO From ctx
        self._rpc_server = self._create_rpc_server()
        ProfiledThread(target=self._rpc_server.serve_forever, name_prefix='SbxRpc').start()

        self._rpc_invoker = ActionQueue(thread_count=10, thread_name_prefix='RpcIO')

# TODO All from ctx
        self._sbx_invoker = ActionQueue(thread_count=10, thread_name_prefix='SbxIO')
        self._sandbox = sandbox.Client(
            ctx.sandbox_api_url, ctx.sandbox_api_token, timeout=15.0, debug=True)
        self._status_awaiter = SandboxTaskStateAwaiter(self._sandbox)

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

# FIXME We can't identify packet by pck_id in async/delayed calls
#       because packet may be recreated by PacketBase
#       (or we must cancel invokers (not only delayed_executor))

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
# TODO XXX Кажется, при resume'инге из _start_snapshot_resource_id нужно
#          использовать не self._executor_resource_id,
#          а тот executor, на котором первый раз запускался пакет
                'executor_resource': self._executor_resource_id,
                'snapshot_data': wrap_string(pck._start_snapshot_data, 79) \
                    if pck._start_snapshot_data \
                    else None,
                'snapshot_resource_id': pck._start_snapshot_resource_id,
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

                pck._exception = e
                pck._mark_terminated()

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

        self._wait_task_state(pck)
        self._do_start_sandbox_task(pck)

    def _do_start_sandbox_task(self, pck):
        try:
            self._sandbox.start_task(pck._sandbox_task_id)

        # Possible events before lock will be acquired in this func:
        # - not final GRAPH_UPDATE
        # - final GRAPH_UPDATE
        # - STOP_GRACEFULLY/STOP/CANCEL

    # XXX Уже неверно.
        # No task status changes may appear here, because awaiting is not racy
        # - final GRAPH_UPDATE + task terminated
        # - task terminated wo GRAPH_UPDATE's

        except Exception as e:
            with pck.lock:
                if pck._state != TaskState.STARTING:
                    return

                # TODO Don't forget to take into account ._target_stop_mode in _on_task_status_change

                # Here we don't know if task is really started
                pck._is_start_error_permanent = \
                    not isinstance(e, (sandbox.NetworkError, sandbox.ServerInternalError))
                    # TODO

                pck._state = TaskState.CHECKING_START_ERROR
                pck._start_error = e

                #pck._wait_task_state(pck, TaskStateGroups.ANY)

            return

        with pck.lock:
            if pck._state != TaskState.STARTING:
                return

            pck._state = TaskState.STARTED

            #self._wait_task_state(pck, TaskStateGroups.TERMINATED)

            assert not pck._peer_addr
            #if self._target_stop_mode:
            #    <waiting for peer addr>
            #
            #    We can't STOP* for now, because we have no _peer_addr
            #    We theoretically can do CANCEL using Sandbox API, but
            #    I prefer do all STOP*/CANCEL in the same way.

            # XXX FIXME If firewall has no holes for us,
            # we can't do STOP*, but theoretically can do CANCEL

    def _wait_task_state(self, pck):
        self._status_awaiter.await(
            pck._sandbox_task_id,
            self._on_task_status_change)

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
            if not pck._sched or pck._sched.id != id:
                return

            pck._sched = None

        # ._sched is None means that impl is running
        # When impl stop it will set ._sched again or modify other fields of pck
        impl(pck)

    def _on_rpc_update_graph(self, task_id, peer_addr, state, is_final):
        pck = self.by_task_id.get(task_id)

        if not pck:
            raise NonExistentInstanceError()

        with pck.lock:
            # FIXME
            # pck: connect
            # pck: write
            # rem: enter _on_rpc_update_graph
            # pck: CRASHED
            # pck: Sandbox' task FAILURE
            # rem: _on_task_status_change (enter + exit)
            # rem: _on_rpc_update_graph with self.lock <-- OOPS

            assert pck._state not in [
                TaskState.CREATING,
                TaskState.TERMINATED,
                TaskState.FETCHING_RESOURCE_LIST,
                TaskState.FETCHING_FINAL_UPDATE
            ]

            if pck._state in [TaskState.STARTING, TaskState.CHECKING_START_ERROR, TaskState.STARTED]:
                if pck._state != TaskState.STARTED:
                    pck._state = TaskState.STARTED
                    pck._drop_sched_if_need()
                    assert not pck._peer_addr
                else:
                    assert not pck._sched

                if not pck._peer_addr:
                    pck._peer_addr = peer_addr

                    if pck._target_stop_mode:
                        if is_final:
                            pck._sent_stop_mode = pck._target_stop_mode # FIXME
                        else:
                            pck._start_packet_stop(pck)

            if pck._target_stop_mode != StopMode.CANCEL:
                pck._update_graph(state, is_final)

            if is_final:
                if pck._target_stop_mode == StopMode.CANCEL \
                    or state['state'] == GraphState.SUCCESSFULL:
                    pck._mark_terminated()
                else:
                    pck._state = TaskState.FETCHING_RESOURCE_LIST
                    self._start_fetch_resource_list(pck)

    def _start_fetch_resource_list(self, pck):
        self._sbx_invoker.invoker(lambda : self._fetch_resource_list(pck))

    def _fetch_resource_list(self, pck):
        try:
            ans = self._sandbox.list_task_resources(pck._sandbox_task_id)
        except:
            logging.exception('Failed to fetch task %d resources' % pck._sandbox_task_id)

            with pck.lock:
                if pck._target_stop_mode != StopMode.CANCEL:
                    self._schedule(pck, self._start_fetch_resource_list, self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)

            return

        res_by_type = {
            resource['type']: resource
                for resource in ans['items']
        }

        with pck.lock:
    # TODO FIXME Don't store/fetch for SUCCESSFULL?
            pck._result_snapshot_resource_id = res_by_type['REM_JOBPACKET_EXECUTION_SNAPSHOT']['id']

            if pck._final_state is None:
                pck._final_update_url = res_by_type['REM_JOBPACKET_GRAPH_UPDATE']['http']['proxy']
                pck._state = PacketState.FETCHING_FINAL_UPDATE
                self._fetch_final_update(pck)
            else:
                pck._mark_terminated()

    def _start_fetch_final_update(pck):
        self._sbx_invoker.invoker(lambda : self._fetch_final_update(pck))

    def _fetch_final_update(pck):
        try:
# TODO timeout
            update_str = requests.get(pck._final_update_url, timeout=30.0)
        except:
            logging.exception('Failed to fetch %s' % pck._final_update_url)
            self._schedule(pck, self._start_fetch_final_update, self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)
            return

        update = pickle.loads(update_str)

        with self.lock:
            if pck._target_stop_mode != StopMode.CANCEL:
                pck._update_graph(state, is_final)

            pck._mark_terminated()

    def _on_task_status_change(self, task_id, status_group):
        #with self.lock:
        if True:
            pck = self.by_task_id.get(task_id)

        if not pck:
            return

        with pck.lock:
            #if pck._status_await_job_id != job_id:
                #return
            #pck._status_await_job_id = None

            state = pck._state

            assert state not in [
                TaskState.CREATING,
                TaskState.TERMINATED,
                TaskState.FETCHING_RESOURCE_LIST,
                TaskState.FETCHING_FINAL_UPDATE
            ]

    # TODO XXX
    # Пока в черне обрисовал

            if state == TaskState.STARTING:
                if status_group == TaskStateGroups.DRAFT:
                    raise AssertionError()

                elif status_group == TaskStateGroups.ACTIVE:
                    pck._state = TaskState.STARTED

                elif status_group == TaskStateGroups.TERMINATED:
                    pck._state = TaskState.FETCHING_RESOURCE_LIST #1
                    self._start_fetch_resource_list(pck)

            elif state == TaskState.CHECKING_START_ERROR:
                # TODO drop_sched

                if status_group == TaskStateGroups.DRAFT:
                    if pck._is_start_error_permanent:
                        pck._mark_terminated()
                    else:
                        pck._state = TaskState.STARTING
                        self._start_start_sandbox_task(pck)

                elif status_group == TaskStateGroups.ACTIVE:
                    pck._state = TaskState.STARTED

                elif status_group == TaskStateGroups.TERMINATED:
                    pck._state = TaskState.FETCHING_RESOURCE_LIST #2
                    self._start_fetch_resource_list(pck)

            elif state == TaskState.STARTED:
                if status_group == TaskStateGroups.DRAFT:
                    raise AssertionError()

                elif status_group == TaskStateGroups.ACTIVE:
                    pass

                elif status_group == TaskStateGroups.TERMINATED:
                    if pck._final_state is not None:
                        pck._mark_terminated()
                    else:
                        pck._state = TaskState.FETCHING_RESOURCE_LIST #1
                        self._start_fetch_resource_list(pck)

    def stop_packet(self, pck, kill_jobs):
        self._stop_packet(pck, StopMode.STOP if kill_jobs else StopMode.STOP_GRACEFULLY)

    def cancel_packet(self, pck):
        self._stop_packet(pck, StopMode.CANCEL)

    def _stop_packet(self, pck, stop_mode):
        with pck.lock:
            if pck._target_stop_mode >= stop_mode:
                return

            def really_cancel():
                pck._drop_sched_if_need()
                pck._mark_terminated()

            pck._target_stop_mode = stop_mode

            state = pck._state

            if state in [TaskState.CREATING, TaskState.FETCHING_RESOURCE_LIST, TaskState.FETCHING_FINAL_UPDATE] \
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

        proxy = rem.xmlrpc.ServerProxy(
            url='http://%s:%d' % pck._peer_addr,
            timeout=15.0) # TODO

        try:
            if stop_mode == StopMode.CANCEL:
                proxy.cancel(task_id)
            else:
                proxy.stop(task_id, kill_jobs=stop_mode == StopMode.STOP)

        except Exception as e:
            logging.exception("Failed to send stop to packet") # XXX For devel
        # TODO ReComment
            #logging.warning("Failed to send stop to packet: %s" % e)

            if is_xmlrpc_exception(e, WrongTaskIdError) \
                    or isinstance(e, socket.error) and e.errno == errno.ECONNREFUSED:
                return # FIXME Is enough?

            with pck.lock:
                if pck._state != TaskState.STARTED:
                    return

                assert not pck._sched

                self._schedule(
                    pck,
                    self._start_packet_stop,
                    timeout=self._RPC_RESEND_INTERVAL)

        else:
            with pck.lock:
                #assert pck._state == TaskState.STARTED # FIXME XXX
                if pck._state != TaskState.STARTED:
                    return

                if pck._sent_stop_mode < stop_mode:
                    pck._sent_stop_mode = stop_mode

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
    FETCHING_FINAL_UPDATE = 9


class AlreadyTerminated(RuntimeError):
    pass


class Unreachable(AssertionError):
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
        self._final_state = False

        remote_packets_dispatcher.register_packet(self)

    def _mark_terminated(self):
        self._state = TaskState.TERMINATED
        self._ops._on_packet_terminated()

    def cancel(self):
        remote_packets_dispatcher.cancel_packet(self)

    #def restart(self): # TODO .fast_restart
        #remote_packets_dispatcher.restart_packet(self)

    def stop(self, kill_jobs):
        remote_packets_dispatcher.stop_packet(self, kill_jobs)

    def get_result(self):
        raise NotImplementedError()

    def _update_graph(self, state, is_final):
        assert not self._cancelled

        self._last_state = state # TODO

        if is_final:
            self._final_state = state['state']

        self._ops._on_sandbox_packet_update(state, is_final)

    def _drop_sched_if_need(self):
        if self._sched:
            self._sched()
            self._sched = None

def _produce_snapshot_data(pck_id, graph):
    pck = sandbox_packet.Packet(pck_id, graph)
    return base64.b64encode(pickle.dumps(pck, 2))


class SandboxJobGraphExecutorProxy(object):
    def __init__(self, ops, pck_id, graph, custom_resources):
        self._ops = ops
        self.lock = self._ops.lock
        self.pck_id = pck_id
        self._graph = graph
        self._custom_resources = custom_resources

        self._remote_packet = None
        self._snapshot_resource_id = None
        #self._result_resource_id = None # TODO FIXME
        self._result_snapshot_resource_id = None

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
    def _on_sandbox_packet_update(self, state, is_final):
        with self.lock:
            if self.cancelled:
                return

# TODO XXX XXX
            pass
            #self._last_state = state # TODO

    # on sandbox task stopped + resources list fetched
    def _on_packet_terminated(self):
        with self.lock:
            self._do_on_packet_terminated()
            self._update_state()

    def _do_on_packet_terminated(self):
        def on_stop():
            self._remote_packet = None
            self._stop_promise.set()
            self._stop_promise = None
            self._ops.on_job_graph_becomes_null()

        #res = self._remote_packet.get_result()
        r = self._remote_packet

        if self.cancelled:
            self.cancelled = False
            self.remote_history = []
            self.detailed_status = {}
            on_stop()
            return

# XXX TODO
# Task FAILURE/EXCEPTION
        #if res.exceptioned: # TODO Rename
            #logging.warning('...')

            ## XXX TODO XXX Rollback history/Status to prev state

            #if self.do_not_run:
                #on_stop()
            #else:
                #self._remote_packet = self._create_remote_packet()

            #return

        # WTF?
        # Even on .do_not_run -- ERROR/SUCCESSFULL more prioritized
        # (TODO Support this rule in PacketBase)

        if r._final_state == GraphState.TIME_WAIT:
            self.time_wait_deadline = r._last_state['nearest_retry_deadline']
            self.time_wait_sched = \
                delayed_executor.schedule(self._stop_time_wait,             # TODO Fix races
                                            deadline=self.time_wait_deadline)

        elif r._final_state == GraphState.SUCCESSFULL:
            self.result = True

        elif r._final_state == GraphState.ERROR:
            self.result = False

        self._result_snapshot_resource_id = r._result_snapshot_resource_id \
            if r._final_state != GraphState.SUCCESSFULL else None

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


