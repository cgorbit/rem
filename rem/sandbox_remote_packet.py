# -*- coding: utf-8 -*-
import os
import sys
import time
import base64
import cPickle
from SimpleXMLRPCServer import SimpleXMLRPCServer
import threading
from Queue import Queue as ThreadSafeQueue

from rem.profile import ProfiledThread
import sandbox_packet
import rem.sandbox as sandbox
from rem_logging import logger as logging
from rem.future import Promise
from packet_state import PacketState

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


class RemotePacketsDispatcher(object):
    _SANDBOX_TASK_CREATION_RETRY_INTERVAL = 10.0

    def __init__(self):
        #self.listen_port = listen_port
        #self.by_pck_id = {}
        self.by_instance_id = {}
# TODO
        self._sbx_task_kill_timeout = 14 * 86400 # TODO
        self._sbx_task_owner = 'guest'
        self._sbx_task_priority = (
            sandbox.TaskPriority.Class.SERVICE,
            sandbox.TaskPriority.SubClass.NORMAL)

    def _create_rpc_server(self):
        srv = SimpleXMLRPCServer(self._rpc_listen_addr)
        srv.register_function(self._rpc_ping, 'ping')
        srv.register_function(self._rpc_set_jobs_statuses, 'set_jobs_statuses')
        return srv

        #def start(self, io_invoker, sandbox):
    def start(self, ctx):
        self._executor_resource_id = ctx.sandbox_executor_resource_id
        self._rpc_listen_addr = ('ws30-511.yandex.ru', 8000) # TODO XXX
        self._rpc_server = self._create_rpc_server()
        ProfiledThread(target=self._rpc_server.serve_forever, name_prefix='SbxRpc').start()

        self._io_invoker = ActionQueue(thread_count=10, thread_name_prefix='SbxIO')
        self.sandbox = sandbox.Client(ctx.sandbox_api_url, ctx.sandbox_api_token, timeout=15.0)

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

    def _rpc_set_jobs_statuses(self, instance_id, jobs, finished_status):
        raise NotImplementedError()

    def _rpc_ping(self, instance_id):
        raise NotImplementedError()

    def register_packet(self, pck):
        with pck.lock:
            pck._sandbox_task_state = SandboxTaskState.CREATING
        self._start_create_sandbox_task(pck)

    def _reschedule_create_sandbox_task(self, pck):
        with pck.lock:
            pck._cancel_schedule = None

            if self._check_cancel_before_starting(pck):
                return

            self._start_create_sandbox_task(pck)


# XXX FIXME We can't identify packet by pck_id in async/delayed calls
#           because packet may be recreated by PacketBase
#           (or we must cancel invokers (not only delayed_executor))

    def _start_create_sandbox_task(self, pck):
# FIXME TODO We can: pck._cancel_create = invoker.invoke(...)
        self._io_invoker.invoke(lambda : self._do_create_sandbox_task(pck))

    def _start_delete_task(self, task_id):
        self._io_invoker.invoke(lambda : self.sandbox.delete_task(task_id)) # no retries

    def _check_cancel_before_starting(self, pck):
        if pck._cancelled:
            pck._sandbox_task_state = SandboxTaskState.NOT_CREATED
            #pck._on_cancelled()
            pck._on_end_of_life()
            return True
        return False

    def _do_create_sandbox_task(self, pck):
        def reschedule_if_need():
            with pck.lock:
                if self._check_cancel_before_starting(pck):
                    return

                pck._cancel_schedule = \
                    delayed_executor.schedule(
                        lambda : self._reschedule_create_sandbox_task(pck),
                        timeout=self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)

        with pck.lock:
            if self._check_cancel_before_starting(pck):
                return

        instance_id = '%s@%.6f' % (pck.id, time.time())

# TODO Handle 500

        def handle_unknown_error():
            raise NotImplementedError()

        try:
            task = self.sandbox.create_task(
                'RUN_REM_JOBPACKET',
                {
                    'executor_resource': self._executor_resource_id,
                    'snapshot_data': wrap_string(pck._snapshot_data, 79) if pck._snapshot_data else None,
                    'snapshot_resource': pck._snapshot_resource_id,
                    'custom_resources': pck._custom_resources,
                    'rem_server_addr': self._rpc_listen_addr,
                    'instance_id': instance_id,
                }
            )
        except (sandbox.NetworkError, sandbox.ServerInternalError) as e:
            reschedule_if_need()
            return
        except:
            logging.exception('')
            handle_unknown_error()
            return

        logging.debug('sbx:%d for %s created' % (task.id, pck.id))

        with pck.lock:
            if self._check_cancel_before_starting(pck):
                return

        try:
            task.update(
                max_restarts=0,
                kill_timeout=self._sbx_task_kill_timeout,
                owner=self._sbx_task_owner,
                priority=self._sbx_task_priority,
                notifications=[],
                description='%s @ %s\n%s\n%s' % (
                    instance_id, self._rpc_listen_addr, '_pck_name_TODO_', '_jobs_graph_TODO_')
                #fail_on_any_error=True, # FIXME What?
            )

        except (sandbox.NetworkError, sandbox.ServerInternalError) as e:
            reschedule_if_need()
            self._start_delete_task(task.id)
            return
        except:
            logging.exception('')
            handle_unknown_error()
            self._start_delete_task(task.id)
            return

        with pck.lock:
            if self._check_cancel_before_starting(pck):
                return

            # FIXME fork locking (mallformed pck state)
            pck._instance_id = instance_id
            pck._sandbox_task_id = task.id
            pck._sandbox_task_state = SandboxTaskState.STARTING

            self.by_instance_id[instance_id] = pck

        try:
            task.start()
        except Exception as e:
            # Here we don't know if task is really started
            if isinstance(e, (sandbox.NetworkError, sandbox.ServerInternalError)):
                func = self._reschedule_sandbox_task_start
                new_state = None
            else:
                func = self._reschedule_sandbox_failed_start_check
                new_state = SandboxTaskState.CHECKING_FAILED_START
                self._start_error = e

            with pck.lock:
                if new_state:
                    pck._sandbox_task_state = new_state
                pck._cancel_schedule = \
                    delayed_executor.schedule(
                        lambda : func(pck),
                        timeout=self._SANDBOX_TASK_CREATION_RETRY_INTERVAL)
            return

        with pck.lock:
            if pck._cancelled:
                pck._sandbox_task_state = SandboxTaskState.STARTED

    def process_incoming_message(self, instance_id, msg):
        with pck.lock:
            pck = self.by_instance_id.get(instance_id)
            if not pck:
                raise NonExistentInstanceError()

            if pck._cancelled:
                return

            raise NotImplementedError()

    def send_message(self, pck):
        raise NotImplementedError()

    def cancel_packet(self, pck):
        with pck.lock:
            if pck._cancelled:
                return

            pck._cancelled = True

            state = pck._sandbox_task_state

            if state == SandboxTaskState.CREATING:
                if pck._cancel_schedule:
                    if pck._cancel_schedule():
                        self._on_end_of_life()
                    pck._cancel_schedule = None

            elif state == SandboxTaskState.NOT_CREATED: # FIXME Must not be
                self._on_end_of_life()

            elif state in [SandboxTaskState.STARTING, SandboxTaskState.CHECKING_FAILED_START]:
                pass # Don't touch!

            else:
                raise NotImplementedError()
            #TERMINATED  = 6
            #CREATION_FAILED = 7


class SandboxTaskState(object):
    NOT_CREATED = 1
    CREATING    = 2
    STARTING    = 3
    STARTED     = 4
    CHECKING_FAILED_START = 5
    TERMINATED  = 6
    CREATION_FAILED = 7


class SandboxRemotePacket(object):
    def __init__(self, ops, pck_id, snapshot_data, snapshot_resource_id, custom_resources):
        self.id = pck_id
        #self._ops = ops
        #self.lock = ops._ops.lock # XXX TODO
        self.lock = threading.Lock()
        self._cancelled = False
        self._sandbox_task_state = SandboxTaskState.NOT_CREATED
        self._instance_id = None
        self._snapshot_resource_id = snapshot_resource_id
        self._snapshot_data = snapshot_data
        self._custom_resources = custom_resources
        self._sandbox_task_id = None
        self._cancel_schedule = None
        self._remote_addr = None

        remote_packets_dispatcher.register_packet(self)

    def cancel(self):
        remote_packets_dispatcher.cancel_packet(self)

    def restart(self):
        remote_packets_dispatcher.restart_packet(self)

    def stop(self, kill_jobs):
        remote_packets_dispatcher.stop_packet(self)


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

        self.do_not_run = True # FIXME
        self.cancelled = False # FIXME
        self.time_wait_deadline = None
        self.time_wait_sched = None
        self.result = None

    # TODO FIXME
        self.detailed_status = None
        self._update_state()
        #self.state = ReprState.PENDING
        #self.history = [] # XXX TODO Непонятно вообще как с этим быть

    def _create_remote_packet(self):
        return SandboxRemotePacket(
            self, # Ops?
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
    # on sandbox task stopped + resources list fetched
    def on_packet_terminated(self, how):
        self._on_packet_terminated(how)
        self._update_state()

    def _on_packet_terminated(self, how):
        def on_stop():
            self._remote_packet = None
            self._stop_promise.set()
            self._stop_promise = None
            self._ops.on_job_graph_becomes_null()

        with self.lock:
            if self.cancelled:
                self.cancelled = False
                on_stop()
                return

            if how.exception:
                logging.warning('...')

                # XXX TODO XXX Rollback history/Status to prev state

                if self.do_not_run:
                    on_stop()
                else:
                    self._remote_packet = self._create_remote_packet()

                return

            # Even on .do_not_run -- ERROR/SUCCESSFULL more prioritized
            # (TODO Support this rule in PacketBase)

            if how == TIME_WAIT:
                self.time_wait_deadline = how.time_wait_deadline
                self.time_wait_sched = \
                    delayed_executor.schedule(self._stop_time_wait,             # TODO Fix races
                                              deadline=self.time_wait_deadline)

            elif how == SUCCESSFULL:
                self.result = True
                self._result_resource_id = how.result_resource_id

            elif how == ERROR:
                self.result = False

            self._snapshot_resource_id = how.snapshot_id \
                if not how.SUCCESSFULL else None

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

    def calc_state(self):
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
                return GraphState.PENDING

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
            if self.do_not_run:
                raise RuntimeError()

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
            self._snapshot_resource_id = None

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
    def __init__(self, pck):
        #self._pck = pck
        self.lock = pck.lock


class _SandboxPacketJobGraphExecutorProxyOps(object):
    def __init__(self, pck):
        self.pck = pck

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


