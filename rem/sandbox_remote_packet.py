# -*- coding: utf-8 -*-
import os
import sys
import time
import base64
import cPickle as pickle
from rem.xmlrpc import AsyncXMLRPCServer2, ServerProxy as XMLRPCServerProxy, \
                       is_xmlrpc_exception, traced_rpc_method
from Queue import Queue as ThreadSafeQueue
import errno
import socket
import xmlrpclib
import json

import requests
import rem.delayed_executor as delayed_executor
from rem.profile import ProfiledThread
import sandbox_packet
import rem.sandbox as sandbox
from rem.sandbox_tasks import TaskStateGroups, SandboxTaskStateAwaiter
from rem_logging import logger as logging
from job_graph import GraphState
from rem.common import PickableLock

remote_packets_dispatcher = None


class WrongTaskIdError(RuntimeError):
    pass


def join_host_port(host, port):
    logging.debug('join_host_port(%s, %s)' % (host, port))
    if ':' in host:
        return '[%s]:%d' % (host, port)
    else:
        return '%s:%d' % (host, port)


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

    put = invoke

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
    _RPC_RESEND_INTERVAL = 20.0

    class TasksAwaiter(SandboxTaskStateAwaiter):
        def __init__(self, sandbox, dispatcher):
            SandboxTaskStateAwaiter.__init__(self, sandbox)
            self.__dispatcher = dispatcher

        def _notify(self, task_id, status_group, can_has_res):
            self.__dispatcher._on_task_status_change(task_id, status_group, can_has_res)

    def __init__(self, rhs=None):
        self._by_task_id = rhs._by_task_id if rhs else {}

    def __getstate__(self):
        return {
            '_by_task_id': self._by_task_id.copy(),
        }

    def __setstate__(self, sdict):
        self._by_task_id = sdict['_by_task_id']

    def _create_rpc_server(self, ctx):
        srv = AsyncXMLRPCServer2(
            ctx.sandbox_rpc_server_thread_pool_size,
            self._rpc_listen_addr,
            allow_none=True)

        #srv.register_function(self._on_rpc_ping, 'ping') # FIXME Don't remember what for
        srv.register_function(self._on_rpc_update_graph, 'update_graph')

        return srv

    def start(self, ctx, alloc_guard):
        self._sbx_task_priority = (
            sandbox.TaskPriority.Class.SERVICE,
            sandbox.TaskPriority.SubClass.NORMAL)

        self._executor_resource_id = ctx.sandbox_executor_resource_id
        self._rpc_listen_addr = ctx.sandbox_rpc_listen_addr
        self._sbx_task_kill_timeout = ctx.sandbox_task_kill_timeout
        self._sbx_task_owner = ctx.sandbox_task_owner
        self._sbx_python_resource_id = ctx.sandbox_python_resource_id

        self._sandbox = ctx.sandbox_client

        self._rpc_invoker = ActionQueue(
            thread_count=ctx.sandbox_rpc_invoker_thread_pool_size,
            thread_name_prefix='RpcIO')

        self._sbx_invoker = ActionQueue(
            thread_count=ctx.sandbox_invoker_thread_pool_size,
            thread_name_prefix='SbxIO')

        self._tasks_status_awaiter = self.TasksAwaiter(self._sandbox, self)

        self._vivify_packets(alloc_guard)

        self._rpc_server = self._create_rpc_server(ctx)
        self._tasks_status_awaiter.start()
        self._rpc_server.start()

    def _vivify_packets(self, alloc_guard):
        logging.debug('RemotePacketsDispatcher packet count: %d' % len(self._by_task_id))

        for pck in self._by_task_id.itervalues():
            logging.debug('VIVIFY %s' % pck.id)
            pck._run_guard = alloc_guard()
            self._await_task_status(pck)
            self._reschedule_packet(pck)

    def _reschedule_packet(self, pck):
# TODO
# TODO CHECK AGAIN
# TODO
        by_state = {
            RemotePacketState.CREATING: self._start_create_sandbox_task,
            RemotePacketState.STARTING: self._start_start_sandbox_task,
            RemotePacketState.FETCHING_RESOURCE_LIST: self._start_fetch_resource_list,
            RemotePacketState.FETCHING_FINAL_UPDATE: self._start_fetch_final_update,
        }

        action = by_state.get(pck._state)

        if not action \
                and pck._target_stop_mode != pck._sent_stop_mode \
                and pck._state == RemotePacketState.STARTED:
            action = self._start_packet_stop

        if action:
            logging.debug('action %s' % action)
            action(pck)

    def stop(self):
        self._rpc_server.shutdown()
        self._rpc_invoker.stop()
        self._sbx_invoker.stop()
        self._tasks_status_awaiter.stop()

# TODO FIXME max_restarts=0
# TODO kill_timeout=14 * 86400
# FIXME fail_on_any_error=False XXX А на каких он не падает?

# TODO Создать рботного пользователя FIXME Правда ли никто не сможет ручками перезапускать?


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
                'rem_server_addr': ('%s:%d' % self._rpc_listen_addr),
                'executor_resource': self._executor_resource_id,
                'snapshot_data': wrap_string(pck._start_snapshot_data, 79) \
                    if pck._start_snapshot_data \
                    else None,
                'snapshot_resource_id': pck._start_snapshot_resource_id,
                # '=' to prevent '[object Object]' rendering of parameter on Sandbox task page
                'custom_resources': '=' + json.dumps(pck._custom_resources, indent=3),
                'python_resource': self._sbx_python_resource_id,
            }
        )

# XXX ACHTUNG
# XXX ACHTUNG
# XXX ACHTUNG
# XXX ACHTUNG
# XXX ACHTUNG
    def _sbx_update_task(self, pck, task):
        real_pck = pck._ops._ops.pck
        prev_task_id = pck._ops._prev_task_id

        jobs = {
            job.id: {
                'command': job.shell,
                'parents': job.parents,
                'pipe_parents': job.inputs,
                'max_try_count': job.max_try_count,
                #'max_working_time': job.max_working_time,
                #retry_delay = retry_delay
                #pipe_fail = pipe_fail
            }
                for job in real_pck.jobs.itervalues()
        }

        description = '''pck_id: {pck_id}
pck_name: {pck_name}
rem_server: {rem_host}:{rem_port}
prev_history: {history}
prev_task: {prev_task}

{job_graph}
'''.format(

            rem_host=self._rpc_listen_addr[0],
            rem_port=self._rpc_listen_addr[1],
            prev_task=prev_task_id,

            pck_id=pck.id,
            pck_name=real_pck.name,
            #pck_name_timestamp_descr=' (1464601024 == 2016-05-30T12:37:20)', # TODO
            history=real_pck.history[:-13], # TODO

            job_graph=json.dumps(jobs, indent=3),
        )

        task.update(
            max_restarts=0,
            kill_timeout=self._sbx_task_kill_timeout,
            owner=self._sbx_task_owner,
            priority=self._sbx_task_priority,
            notifications=[],
            description=description,
            #fail_on_any_error=True, # FIXME What?
        )

    def _mark_all_done(self, pck, reason=None):
        if pck._state != RemotePacketState.TASK_FIN_WAIT:
            pck._ops._on_packet_terminated() # TODO Ugly

        pck._set_state(RemotePacketState.ALL_DONE, reason)
        pck._run_guard = None # j.i.c
        #self._tasks_status_awaiter.cancel_wait(pck._sandbox_task_id) # TODO XXX NotImplementedError

        if pck._sandbox_task_id:
            self._by_task_id.pop(pck._sandbox_task_id)

    def _mark_task_fin_wait(self, pck, reason=None):
        pck._set_state(RemotePacketState.TASK_FIN_WAIT, reason)
        pck._ops._on_packet_terminated() # TODO Ugly

    def _do_create_sandbox_task(self, pck):
        def reschedule_if_need():
            with pck._lock:
                if pck._target_stop_mode:
                    return

                self._schedule(
                    pck,
                    self._start_create_sandbox_task,
                    timeout=self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)

        with pck._lock:
            if pck._target_stop_mode:
                return

        def handle_unknown_error(e):
            with pck._lock:
                if not pck._target_stop_mode:
                    pck._error = e
                self._mark_all_done(pck, 'Unknown error while creating: %s' % e)

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

        with pck._lock:
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

        with pck._lock:
            if pck._target_stop_mode:
                return

            # FIXME fork locking (mallformed pck state)
            pck._sandbox_task_id = task.id
            pck._set_state(RemotePacketState.STARTING)

            self._by_task_id[task.id] = pck

        self._await_task_status(pck)
        self._do_start_sandbox_task(pck)

    def _await_task_status(self, pck):
        self._tasks_status_awaiter.await(pck._sandbox_task_id)

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
            with pck._lock:
                if pck._state != RemotePacketState.STARTING:
                    return

                # TODO Don't forget to take into account ._target_stop_mode in _on_task_status_change

                # Here we don't know if task is really started
                pck._is_start_error_permanent = \
                    not isinstance(e, (sandbox.NetworkError, sandbox.ServerInternalError))
                    # TODO

                pck._set_state(RemotePacketState.CHECKING_START_ERROR)
                pck._error = e

            return

        with pck._lock:
            if pck._state != RemotePacketState.STARTING:
                return

            pck._set_state(RemotePacketState.STARTED, '._sandbox.start() -> ok')

            assert not pck._peer_addr
            #if pck._target_stop_mode:
            #    <waiting for peer addr>
            #
            #    We can't STOP* for now, because we have no _peer_addr
            #    We theoretically can do CANCEL using Sandbox API, but
            #    I prefer do all STOP*/CANCEL in the same way.

            # XXX FIXME If firewall has no holes for us,
            # we can't do STOP*, but theoretically can do CANCEL

# The hard way
    # must be called under lock
    #def _schedule(self, pck, impl, timeout=None):
        #id = None
        #wrap = lambda : self._execute_scheduled(pck, id, impl)
        #id = id(wrap)
        #cancel = delayed_executor.schedule(wrap, timeout=timeout)

        #pck._sched = (id, cancel)

    #def _execute_scheduled(self, pck, id, impl):
        #with pck._lock:
            #if not pck._sched or pck._sched[0] != id:
                #return

            #pck._sched = None

            #impl()

    # must be called under lock
    def _schedule(self, pck, impl, timeout=None):
        wrap = lambda id: self._execute_scheduled(pck, id, impl)
        pck._sched = delayed_executor.schedule(wrap, timeout=timeout)

    def _execute_scheduled(self, pck, id, impl):
        with pck._lock:
            if not pck._sched or pck._sched.id != id:
                return

            pck._sched = None

        # ._sched is None means that impl is running
        # When impl stop it will set ._sched again or modify other fields of pck
        impl(pck)

    @traced_rpc_method()
    def _on_rpc_update_graph(self, task_id, peer_addr, state, is_final):
        import pprint
        logging.debug('_ON_RPC_UPDATE_GRAPH[%s]: %s' % (
            GraphState.str(state['state']),
            pprint.pformat({
                'task_id': task_id,
                'peer_addr': peer_addr,
                'is_final': is_final,
                'state': state
            })
        ))

        pck = self._by_task_id.get(task_id)

        if not pck:
            raise WrongTaskIdError()

        with pck._lock:
            # FIXME
            # pck: connect
            # pck: write
            # rem: enter _on_rpc_update_graph
            # pck: CRASHED
            # pck: Sandbox' task FAILURE
            # rem: _on_task_status_change (enter + exit)
            # rem: _on_rpc_update_graph with self.lock <-- OOPS

            logging.debug('_on_rpc_update_graph state %s' % pck._state)

            assert pck._state not in [
                RemotePacketState.CREATING,
                RemotePacketState.TASK_FIN_WAIT,
                RemotePacketState.ALL_DONE,
                RemotePacketState.FETCHING_RESOURCE_LIST,
                RemotePacketState.FETCHING_FINAL_UPDATE
            ], "_on_rpc_update_graph in %s state" % pck._state

            if pck._state in [RemotePacketState.STARTING,
                              RemotePacketState.CHECKING_START_ERROR,
                              RemotePacketState.STARTED]:
                if pck._state != RemotePacketState.STARTED:
                    pck._set_state(RemotePacketState.STARTED, '_on_rpc_update_graph')
                    pck._drop_sched_if_need()
                    assert not pck._peer_addr
                else:
                    assert not pck._sched

                if not pck._peer_addr:
                    pck._peer_addr = peer_addr
                    logging.debug('SET pck._peer_addr = %s' % (peer_addr,))

                    if pck._target_stop_mode:
                        if is_final:
                            pck._sent_stop_mode = pck._target_stop_mode # FIXME
                        else:
                            self._start_packet_stop(pck)

            if pck._target_stop_mode != StopMode.CANCEL:
                pck._update_graph(state, is_final)

            if is_final:
                if pck._target_stop_mode == StopMode.CANCEL \
                    or state['state'] == GraphState.SUCCESSFULL:
                    self._mark_task_fin_wait(pck, '_on_rpc_update_graph(SUCCESSFULL)')
                else:
                    pass # XXX WAITING for TaskStateGroups.TERMINATED

    def _start_fetch_resource_list(self, pck):
        self._sbx_invoker.invoke(lambda : self._fetch_resource_list(pck))

# TODO XXX FIXME From which task state resources are ready?
    def _fetch_resource_list(self, pck):
        try:
            ans = self._sandbox.list_task_resources(pck._sandbox_task_id)
        except:
            logging.exception('Failed to fetch task %d resources' % pck._sandbox_task_id)

            with pck._lock:
                if pck._target_stop_mode != StopMode.CANCEL:
                    self._schedule(pck, self._start_fetch_resource_list, self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)

            return

# TODO We don't have to _fetch_resource_list() in any TERMINATED task state (e.g. ERROR, EXCEPTION)

        #import json
        #logging.debug('task #%s resources list answer: %s' % (pck._sandbox_task_id, json.dumps(ans, indent=3)))

        res_by_type = {
            resource['type']: resource
                for resource in ans['items']
        }

        #logging.debug('task #%s res_by_type: %s' % (pck._sandbox_task_id, json.dumps(res_by_type, indent=3)))

# TODO XXX XXX Handle KeyError: 'REM_JOBPACKET_EXECUTION_SNAPSHOT'

        with pck._lock:
            pck._result_snapshot_resource_id = res_by_type['REM_JOBPACKET_EXECUTION_SNAPSHOT']['id']

            if pck._final_state is None:
                pck._final_update_url = res_by_type['REM_JOBPACKET_GRAPH_UPDATE']['http']['proxy']
                pck._set_state(RemotePacketState.FETCHING_FINAL_UPDATE)
            else:
                self._mark_all_done(pck, '_fetch_resource_list')
                return

        self._fetch_final_update(pck) # not under lock

    def _start_fetch_final_update(self, pck):
        self._sbx_invoker.invoke(lambda : self._fetch_final_update(pck))

    def _fetch_final_update(self, pck):
        def log_fail(error):
            logging.error('Failed to fetch %s: %s' % (pck._final_update_url, error))

        def reschedule_if_need():
            with pck._lock:
                if pck._target_stop_mode == StopMode.CANCEL:
                    return

                self._schedule(pck,
                            self._start_fetch_final_update,
                            self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)

        try:
# TODO timeout
            resp = requests.get(pck._final_update_url, timeout=30.0)
        except Exception as e:
# FIXME permanent errors?
            log_fail(e)
            reschedule_if_need()
            return

        if resp.status_code != 200:
            http_status_group = resp.status_code / 100

            if http_status_group == 5:
                log_fail(e)
                reschedule_if_need()
                return

            elif http_status_group == 3:
                raise NotImplementedError()

            else:
                log_fail('http status code == %d' % resp.status_code)

                with pck._lock:
                    if pck._target_stop_mode != StopMode.CANCEL:
                        pck._error = RuntimeError() # TODO
                    self._mark_all_done(pck)

                return

        try:
            update = pickle.loads(resp.content)
        except Exception as e:
            log_fail('malformed dump: %s' % e)

            with pck._lock:
                if pck._target_stop_mode != StopMode.CANCEL:
                    pck._error = RuntimeError('Malformed last update resource data: %s' % e)
                self._mark_all_done(pck)

            return


        with pck._lock:
            if pck._target_stop_mode != StopMode.CANCEL:
                pck._update_graph(update, is_final=True)

            self._mark_all_done(pck, '_fetch_final_update')

    def _on_task_status_change(self, task_id, status_group, can_has_res):
        #with self.lock:
        if True:
            pck = self._by_task_id.get(task_id)

        if not pck:
            return

        with pck._lock:
            #if pck._status_await_job_id != job_id:
                #return
            #pck._status_await_job_id = None

            state = pck._state

            assert state not in [
                RemotePacketState.ALL_DONE,
                RemotePacketState.FETCHING_RESOURCE_LIST,
                RemotePacketState.FETCHING_FINAL_UPDATE
            ]

            if state in [
                RemotePacketState.CREATING, # we subscribe just after creation and before start
            ]:
                return

    # TODO XXX
    # Пока в черне обрисовал

            if status_group == TaskStateGroups.DRAFT:

                if state == RemotePacketState.STARTING:
                    pass

                elif state == RemotePacketState.CHECKING_START_ERROR:
                    if pck._is_start_error_permanent:
                        self._mark_all_done(pck, '_is_start_error_permanent=True, DRAFT')
                    else:
                        pck._error = None
                        pck._set_state(RemotePacketState.STARTING)
                        self._start_start_sandbox_task(pck)

                elif state in [RemotePacketState.STARTED, RemotePacketState.TASK_FIN_WAIT]:
# FIXME Race here between graph updates and _on_task_status_change
                    logging.warning('_on_task_status_change(%s, %s)' % (status_group, state))
                    #raise AssertionError()

            elif status_group == TaskStateGroups.ACTIVE:

                if state in [RemotePacketState.STARTING, RemotePacketState.CHECKING_START_ERROR]:
                    pck._set_state(RemotePacketState.STARTED, 'TaskStateGroups.ACTIVE')

            elif status_group == TaskStateGroups.TERMINATED:

                if state in [RemotePacketState.STARTING,
                             RemotePacketState.CHECKING_START_ERROR,
                             RemotePacketState.STARTED]:

                    pck._set_state(RemotePacketState.FETCHING_RESOURCE_LIST)
                    self._start_fetch_resource_list(pck)

                elif state == RemotePacketState.TASK_FIN_WAIT:
                    self._mark_all_done(pck, 'Task TERMINATED on TASK_FIN_WAIT')


    def restart_packet(self, pck):
        return # TODO RPC
        raise NotImplementedError()

    def resume_packet(self, pck):
        return # TODO RPC
        raise NotImplementedError()

    def stop_packet(self, pck, kill_jobs):
        self._stop_packet(pck, StopMode.STOP if kill_jobs else StopMode.STOP_GRACEFULLY)

    def cancel_packet(self, pck):
        self._stop_packet(pck, StopMode.CANCEL)

    def _stop_packet(self, pck, stop_mode):
# TODO
# TODO CHECK AGAIN
# TODO
        with pck._lock:
            if pck._target_stop_mode >= stop_mode:
                return

            def really_cancel():
                pck._drop_sched_if_need()
                self._mark_all_done(pck, '_stop_packet')

            pck._target_stop_mode = stop_mode

            state = pck._state

            if state == RemotePacketState.ALL_DONE \
                or state == RemotePacketState.TASK_FIN_WAIT \
                    and pck._final_state == GraphState.SUCCESSFULL:
                raise AlreadyTerminated()

            elif state == RemotePacketState.CREATING \
                or state == RemotePacketState.STARTING and pck._sched:

                really_cancel()

            elif stop_mode == StopMode.CANCEL \
                and state in [
                    RemotePacketState.FETCHING_RESOURCE_LIST,
                    RemotePacketState.FETCHING_FINAL_UPDATE,
                    RemotePacketState.TASK_FIN_WAIT]:

                really_cancel()

            # FIXME
            elif state == RemotePacketState.STARTED:
                if pck._peer_addr:
                    self._start_packet_stop(pck)

            #elif state in [RemotePacketState.STARTING, RemotePacketState.CHECKING_START_ERROR]:
                #pass # later

            #else:
                #raise Unreachable()

    def _do_stop_packet(self, pck):
        task_id = pck._sandbox_task_id
        stop_mode = pck._target_stop_mode

# TODO
        assert pck._peer_addr is not None

        proxy = XMLRPCServerProxy(
            uri='http://%s' % join_host_port(*pck._peer_addr),
            timeout=15.0
        )

        logging.debug('_do_stop_packet(pck=%s, task=%s, stop_mode=%s' % (
            pck.id, task_id, stop_mode))

        try:
            if stop_mode == StopMode.CANCEL:
                proxy.cancel(task_id)
            else:
                kill_jobs = stop_mode == StopMode.STOP
                proxy.stop(task_id, kill_jobs)

        except Exception as e:
            logging.exception("Failed to send stop to packet") # XXX For devel
        # TODO ReComment
            #logging.warning("Failed to send stop to packet: %s" % e)

            if is_xmlrpc_exception(e, WrongTaskIdError) \
                    or isinstance(e, socket.error) and e.errno == errno.ECONNREFUSED:
                return # FIXME Is enough?

            with pck._lock:
                if pck._state != RemotePacketState.STARTED:
                    return

                assert not pck._sched

                self._schedule(
                    pck,
                    self._start_packet_stop,
                    timeout=self._RPC_RESEND_INTERVAL)

        else:
            with pck._lock:
                #assert pck._state == RemotePacketState.STARTED # FIXME XXX
                if pck._state != RemotePacketState.STARTED:
                    return

                if pck._sent_stop_mode < stop_mode:
                    pck._sent_stop_mode = stop_mode

    def _start_packet_stop(self, pck):
        logging.debug('_start_packet_stop(%s, %s)' % (pck.id, pck._target_stop_mode))
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


class RemotePacketState(object):
    CREATING = 1
    STARTING = 3 # after .create_task()+task.update() till we know, that task started
    CHECKING_START_ERROR = 4 # permanent or temporary error
    STARTED = 6
    TASK_FIN_WAIT = 7 # final update by RPC was received, but task not finished yet
    FETCHING_RESOURCE_LIST = 8 # final update was missed
    FETCHING_FINAL_UPDATE = 9 # --//--
    #HAS_RESULT = 10 # task finished and we have final update (by RPC or through FETCHING*)
    ALL_DONE = 10 # task finished and we have final update (by RPC or through FETCHING*)

    _NAMES = {
        CREATING: 'CREATING',
        STARTING: 'STARTING',
        CHECKING_START_ERROR: 'CHECKING_START_ERROR',
        STARTED: 'STARTED',
        TASK_FIN_WAIT: 'TASK_FIN_WAIT',
        FETCHING_RESOURCE_LIST: 'FETCHING_RESOURCE_LIST',
        FETCHING_FINAL_UPDATE: 'FETCHING_FINAL_UPDATE',
        #HAS_RESULT: 'HAS_RESULT',
        ALL_DONE: 'ALL_DONE',
    }


class AlreadyTerminated(RuntimeError):
    pass


class Unreachable(AssertionError):
    pass


class SandboxRemotePacket(object):
    def __init__(self, ops, pck_id, run_guard, snapshot_data,
                 snapshot_resource_id, custom_resources):
        self.id = pck_id
        self._run_guard = run_guard
        self._ops = ops
        self._lock = PickableLock()
        self._state = RemotePacketState.CREATING
        self._start_snapshot_resource_id = snapshot_resource_id
        self._start_snapshot_data = snapshot_data
        self._custom_resources = custom_resources

        self._target_stop_mode = StopMode.NONE
        self._sent_stop_mode   = StopMode.NONE # at least helpfull for backup loading

        self._sandbox_task_id = None
        self._sched = None
        self._peer_addr = None
        self._error = None
        self._is_start_error_permanent = None
        self._final_state = None
        self._result_snapshot_resource_id = None
        #self._task_awaited = False

        self._succeeded_jobs = set()

        remote_packets_dispatcher.register_packet(self)

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict['_run_guard'] = None
        sdict['_sched'] = None
        return sdict

    def _set_state(self, state, reason=None):
        self._state = state
        logging.debug('remote-%s new state %s, reason: %s' % (self.id, RemotePacketState._NAMES[state], reason))

    def cancel(self):
        remote_packets_dispatcher.cancel_packet(self)

    def stop(self, kill_jobs):
        remote_packets_dispatcher.stop_packet(self, kill_jobs)

    # Try to resume packet while it's stopping (use changed his mind)
    def resume(self):
        remote_packets_dispatcher.resume_packet(self)

    # Fast restart without task recreation
    def restart(self):
        remote_packets_dispatcher.restart_packet(self)

    #def get_result(self):
        #raise NotImplementedError()

    def _update_graph(self, update, is_final):
        assert self._target_stop_mode != StopMode.CANCEL

        self._last_update = update # TODO

        succeed_jobs = set(map(int, update['succeed_jobs']))

        new_succeed_jobs = succeed_jobs - self._succeeded_jobs
        self._succeeded_jobs = succeed_jobs

        if is_final:
            self._final_state = update['state']

            logging.debug('SandboxRemotePacket._final_state = %s' \
                % GraphState.str(self._final_state))

        self._ops._on_sandbox_packet_update(update, new_succeed_jobs, is_final)

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
        self._prev_task_id = None
        self._remote_state = None
        self._prev_snapshot_resource_id = None
        self._error = None

        self.cancelled = False # FIXME
        self.stopping = False # FIXME

        self.time_wait_deadline = None
        self.time_wait_sched = None
        self.result = None

    # TODO FIXME
        self.remote_history = [] # TODO
        self.detailed_status = {}
        self.state = None
        #self.history = [] # XXX TODO Непонятно вообще как с этим быть

    def init(self):
        self._update_state()

    def _create_remote_packet(self, guard):
        return SandboxRemotePacket(
            ops=self,
            run_guard=guard,
            pck_id=self.pck_id,
            snapshot_data=_produce_snapshot_data(self.pck_id, self._graph) \
                if not self._prev_snapshot_resource_id else None,
            snapshot_resource_id=self._prev_snapshot_resource_id,
            custom_resources=self._custom_resources
        )

    #def get_state(self):
        #if not self.history:
            #return ReprState.PENDING
        #return self.history[-1][0]

    def produce_detailed_status(self):
        return self.detailed_status

    #def is_stopping(self):
        #with self.lock:
            #return self.cancelled and self._remote_packet
            # (WORKING | CANCELLED) || (WORKING | SUSPENDED)

    def is_cancelling(self):
        return self.cancelled

    def is_stopping(self):
        return self.stopping

########### Ops for _remote_packet
    def _on_sandbox_packet_update(self, update, succeed_jobs, is_final):
        with self.lock:
            if self.cancelled:
                return

# TODO XXX XXX
            self.detailed_status = update['detailed_status']
            self._remote_state = update['state']

            # set(map(int, state['succeed_jobs'])) # TODO
            # state['state'] # TODO

            for job_id in succeed_jobs:
                self._ops.job_done_successfully(job_id)

            self._update_state()

    # on sandbox task stopped + resources list fetched
    def _on_packet_terminated(self):
        with self.lock:
            self._do_on_packet_terminated()
            self._update_state()

    def _do_on_packet_terminated(self):
        def on_stop():
            self._prev_task_id = self._remote_packet._sandbox_task_id
            self._remote_packet = None
            self._remote_state = None
            #self._ops.on_job_graph_becomes_null()

        assert not self.time_wait_deadline and not self.time_wait_sched

        #res = self._remote_packet.get_result()
        r = self._remote_packet

        self.stopping = False

        if self.cancelled:
            self.cancelled = False
            on_stop()
            return

# XXX TODO
# XXX TODO
# Task FAILURE/EXCEPTION
        #if res.exceptioned: # TODO Rename
            #logging.warning('...')

            ## XXX TODO XXX Rollback history/Status to prev state


        # WTF?
        # Even on .do_not_run -- ERROR/SUCCESSFULL more prioritized
        # (TODO Support this rule in PacketBase)

        logging.debug('state for SandboxJobGraphExecutorProxy == %s' \
            % None if r._final_state is None else GraphState.str(r._final_state))

        if r._error:
            self._error = r._error

        elif r._final_state == GraphState.TIME_WAIT:
            self.time_wait_deadline = r._last_update['nearest_retry_deadline']
            self.time_wait_sched = \
                delayed_executor.schedule(self._stop_time_wait,
                                            deadline=self.time_wait_deadline)

        elif r._final_state == GraphState.SUCCESSFULL:
            self.result = True

        elif r._final_state == GraphState.ERROR:
            self.result = False

# XXX 'prev' was for auto-restart
        self._prev_snapshot_resource_id = r._result_snapshot_resource_id \
            if r._final_state != GraphState.SUCCESSFULL else None

        on_stop()

    def _stop_time_wait(self, sched_id):
        with self.lock:
            if not self.time_wait_sched or self.time_wait_sched.id != sched_id:
                return

            self.time_wait_deadline = None
            self.time_wait_sched = None

            assert not self.cancelled

            #if not self.do_not_run:
                #self._remote_packet = self._create_remote_packet()

            self._update_state()

    def _update_state(self):
        new = self._calc_state()
        if new == self.state:
            return

        self.state = new
        logging.debug('SandboxJobGraphExecutorProxy.state => %s' % GraphState.str(self.state))
        self._ops.on_state_change()

    def _calc_state(self):
        with self.lock:
            if self._remote_packet:
        # XXX
                if not self.cancelled and self._remote_state == GraphState.TIME_WAIT:
                    return self._remote_state

                state = GraphState.WORKING

                if self.cancelled:
                    state |= GraphState.CANCELLED
                #elif self.do_not_run:
                    #state |= GraphState.SUSPENDED

                return state

            elif self.result is not None:
                return GraphState.SUCCESSFULL if self.result else GraphState.ERROR

            elif self._error:
# TODO XXX
                return GraphState.ERROR

            elif self.time_wait_deadline:
                return GraphState.TIME_WAIT

            #elif self.do_not_run:
                #return GraphState.SUSPENDED

            else:
                return GraphState.PENDING_JOBS

    def get_worker_state(self):
        return self._remote_state

    def stop(self, kill_jobs):
        with self.lock:
            if not self._remote_packet:
                #raise RuntimeError("Nothing to stop")
                return

            if self.cancelled:
                return # FIXME

            self.stopping = True
            #self.do_not_run = True
            self._remote_packet.stop(kill_jobs)
            self._update_state()

    def try_soft_resume(self):
        with self.lock:
            self._remote_packet.resume()

    def try_soft_restart(self):
        with self.lock:
            self._remote_packet.restart()

    def start(self, guard):
        with self.lock:
            if self._remote_packet:
                raise RuntimeError()

            #self.do_not_run = False

            self._remote_packet = self._create_remote_packet(guard)

            self._update_state()

# XXX BEAUTY
    def cancel(self):
        self._cancel(False)

# XXX BEAUTY
    def reset(self):
        self._cancel(True)

# XXX BEAUTY
    def _cancel(self, need_reset):
        with self.lock:
            if need_reset:
                self._prev_snapshot_resource_id = None
                self.result = None
                self.remote_history = []
                self.detailed_status = {}

            if not self._remote_packet:
                return

            if self.cancelled:
                return

            #self.do_not_run = True
            self.cancelled = True

            if self.time_wait_sched:
                self.time_wait_sched()
                self.time_wait_sched = None
                self.time_wait_deadline = None

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

    def job_done_successfully(self, job_id):
        tag = self.pck.job_done_tag.get(job_id)
        if tag:
            tag.Set()

    def create_job_runner(self, job):
        raise AssertionError()

    #def on_job_graph_becomes_null(self):
        #self.pck._update_state() # FIXME


