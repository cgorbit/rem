import time
import threading
import copy

import rem.job
import rem.job_graph
from rem.job_graph import GraphState
from rem.profile import ProfiledThread
#import rem.packet # XXX don't import
import rem.subprocsrv
from rem_logging import logger as logging


class _ExecutorOps(object):
    def __init__(self, pck):
        self.pck = pck

    def stop_waiting(self, stop_id):
        self.pck._stop_waiting(stop_id)

    def del_working(self, job):
        pass

    def add_working(self, job):
        pass

    def get_io_directory(self):
        return self.pck.get_io_directory()

    def on_state_change(self):
        pck = self.pck

        pck._update_state()
        logging.debug('pck._has_updates = True')
        pck._has_updates = True

        pck._mark_as_finished_if_need()

        logging.debug('pck._something_changed.notify()')
        pck._something_changed.notify()

    def job_done_successfully(self, job_id):
        self.pck._has_updates = True
        self.pck._something_changed.notify()

    def create_job_runner(self, job):
        pck = self.pck
        return rem.job.JobRunner(pck, job, pck._get_vaults_for(job.id)) # brain damage


class Packet(object):
    _MAX_TIME_WAIT = 60.0 # FIXME

    def __init__(self, pck_id, graph):
        self.id = pck_id
        self.name = '_TODO_packet_name_for_%s' % pck_id # TODO
        self.history = []
        self._init_non_persistent()

        self.state = None
        #self._update_state_if_need()

        self._graph_executor = rem.job_graph.JobGraphExecutor(
            _ExecutorOps(self),
            self.id,
            graph,
        )

    # TODO Better
        with self._lock:
            self._graph_executor.init()

    def _get_vaults_for(self, job_id):
        vaults = self._vaults_setup
        if not vaults:
            return None

        env = copy.copy(vaults['global']) or {}
        env.update(vaults['jobs'].get(job_id, {}))

        logging.debug('Vaults for %d: %s' % (job_id, env))

        return env

    def _mark_as_finished_if_need(self):
        graph = self._graph_executor

        self._finished = \
            graph.state in [GraphState.SUCCESSFULL, GraphState.ERROR] \
            or graph.state == GraphState.TIME_WAIT \
                and graph.get_nearest_retry_deadline() - time.time() > self._MAX_TIME_WAIT \
            or (self._do_not_run or self._cancelled) and graph.is_null()

    def _update_state(self):
        new_state = self._calc_state()
        if new_state == self.state:
            return
        self.state = new_state
        self.history.append((new_state, time.time()))
        logging.info("new state %s" % new_state)

    def _init_non_persistent(self):
        self._lock = threading.RLock()
        self._something_changed = threading.Condition(self._lock)
        self._main_thread = None
        self._job_threads = []
        self._proc_runner = None
        self._do_not_run = False

        self._finished = False
        self._cancelled = False
        self._has_updates = False

    def vivify_jobs_waiting_stoppers(self):
        with self._lock:
            self._graph_executor.vivify_jobs_waiting_stoppers()

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict.pop('_lock', None)
        sdict.pop('_something_changed', None)
        sdict.pop('_working_directory', None)
        sdict.pop('_io_directory', None)
        sdict.pop('_main_thread', None)
        sdict.pop('_proc_runner', None)
        sdict.pop('_job_threads', None)
        sdict.pop('_on_update', None)
        sdict.pop('_do_not_run', None)

        sdict.pop('_finished', None)
        sdict.pop('_cancelled', None)
        sdict.pop('_has_updates', None)
        return sdict

    def __setstate__(self, sdict):
        self.__dict__.update(sdict)
        self._init_non_persistent()

    def start(self, working_directory, io_directory, on_update, reset_tries=False,
              vaults_setup=None):
        self._on_update = on_update
        self._working_directory = working_directory
        self._io_directory = io_directory
        #self._init_non_persistent()
        self._proc_runner = rem.job.create_job_runner(None, None)

        if vaults_setup:
            def dictify(pairs):
                if pairs is None:
                    return None
                return dict(pairs)

            vaults_setup = {
                'global': dictify(vaults_setup['global']),
                'jobs': {
                    int(job_id): dictify(setup)
                        for job_id, setup in vaults_setup['jobs'].items()
                }
            }

        self._vaults_setup = vaults_setup

        with self._lock:
            if reset_tries:
                self._graph_executor.reset_tries()
            #self.resume()
            #self._do_not_run = False

        self._main_thread = ProfiledThread(target=self._main_loop, name_prefix='PckLoop')
        self._main_thread.start()

    def join(self):
        self._main_thread.join()

    def get_working_directory(self):
        return self._working_directory

    def get_io_directory(self):
        return self._io_directory

    def _start_one_another_job(self):
        logging.debug('+ Packet._start_one_another_job')
        job_runner = self._graph_executor.get_job_to_run()
        t = ProfiledThread(target=job_runner.run, name_prefix='Job')
        self._job_threads.append(t)
        t.start()

    def stop(self, kill_jobs):
        with self._lock:
            #if self._do_not_run: # XXX May be called with different kill_jobs
                #return
            if self._finished: # FIXME
                #raise RuntimeError("Already finished")
                return
            if self._cancelled:
                raise RuntimeError("Already cancelled")

            self._do_not_run = True

            if kill_jobs:
                self._graph_executor.cancel()
                self._mark_as_finished_if_need()

            self._something_changed.notify()

    # For those who changed their's minds after call to stop(kill_jobs=False)
    def resume(self):
        with self._lock:
            if self._finished: # FIXME
                raise RuntimeError("Already finished")
            if self._cancelled:
                raise RuntimeError("Already cancelled")

            if self._do_not_run:
                self._do_not_run = False
                self._graph_executor.reset_tries()
                self._mark_as_finished_if_need()
                self._something_changed.notify()

    def cancel(self):
        with self._lock:
            if self._finished:
                raise RuntimeError("Already finished")
            self._cancelled = True
            self._graph_executor.cancel()
            self._mark_as_finished_if_need()
            self._something_changed.notify()

    def is_cancelled(self):
        return self._cancelled

    def restart(self):
        with self._lock:
            if self._finished:
                raise RuntimeError("Already finished")
            if self._cancelled:
                raise RuntimeError("Already cancelled")
            self._do_not_run = False # was any
            self._graph_executor.reset()
            self._something_changed.notify()

    def produce_rem_update_message(self):
        graph = self._graph_executor

        state = {
            #'history': list(self.history), # TODO FIXME
            'state': self.state,
            'detailed_status': graph.produce_detailed_status(),
            'succeed_jobs': map(str, graph.get_succeeded_jobs()),
        }

        if graph.state == GraphState.TIME_WAIT:
            state['nearest_retry_deadline'] = graph.get_nearest_retry_deadline()

        return state

    def _send_update(self):
        self._on_update(self.produce_rem_update_message())

    def _main_loop(self):
        logging.debug('+ Packet.run')

        while True:
            with self._lock:
                logging.debug('_before_job_start_loop')

                if not (self._do_not_run or self._cancelled):
                    logging.debug('_graph_executor.state == %s' \
                        % GraphState.str(self._graph_executor.state))

                    while self._graph_executor.state & GraphState.PENDING_JOBS:
                        self._start_one_another_job()

                logging.debug('_before_send_update_check: %s' % ((self._has_updates, self._finished),))
                if self._has_updates and not self._finished:
                    logging.debug('_before_send_update')
                    self._send_update()
                    self._has_updates = False

                if self._finished:
                    break

                logging.debug('_before_cond_wait')
                self._something_changed.wait()
                logging.debug('_after_cond_wait')

        logging.debug('+ exiting Packet.run')

    def _calc_state(self):
        graph = self._graph_executor
        return graph.state # FIXME

        if graph.is_null():
            if self._do_not_run:
                return GraphState.SUSPENDED
            elif self._cancelled:
                return GraphState.CANCELLED

        return graph.state

    def _stop_waiting(self, stop_id):
        with self._lock:
            if self._cancelled or self._finished: # FIXME _do_not_run
                return
            self._graph_executor.stop_waiting(stop_id)

# OPS for rem.job.Job
    def start_process(self, *args, **kwargs):
        return self._proc_runner(*args, **kwargs)

    def notify_long_execution(self, job):
        raise NotImplementedError()

    def _create_job_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)

    def on_job_done(self, job_runner):
        self._graph_executor.on_job_done(job_runner)

        with self._lock:
            self._graph_executor.apply_jobs_results()

    def create_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)
