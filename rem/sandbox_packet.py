import time
import json
import threading

import rem.job
import rem.job_graph
#import rem.packet # XXX don't import
import rem.subprocsrv
from rem.packet_common import ReprState
from rem_logging import logger as logging

class Resources(object):
    def __init__(self):
        self.added_files = [] # (id, digest, filename)
        self.python = None
        self.packet_executor = None


class Packet(object):
    def __init__(self, pck_id, rem_server, jobs, jobs_graph, kill_all_jobs_on_error):
        self.id = pck_id
        self.name = '_TODO_packet_name_for_%s' % pck_id # TODO
        self.rem_server = rem_server # FIXME rem_server in deserialize?
        self.kill_all_jobs_on_error = kill_all_jobs_on_error
        self.jobs = jobs
        self.jobs_graph = jobs_graph
        self.history = []
        self.state = ReprState.CREATED

    def _create_job_graph_executor(self, runner, directory):
        return rem.job_graph.JobGraphExecutor(
            _ExecutorOps(runner),
            self.jobs,
            self.jobs_graph,
            self.id,
            directory,
            self.kill_all_jobs_on_error,
        )

    def __get_sbx_state__(self):
        sdict = self.__dict__.copy()

        sdict['jobs'] = {
            id: job.__get_sbx_state__()
                for id, job in sdict['jobs'].items()
        }

        return sdict

    def __set_sbx_state__(self, sdict):
        self.__dict__.clear()
        self.__dict__.update(sdict)

    def serialize(self, out):
        json.dump(self.__get_sbx_state__(), out, indent=3)

    def deserialize(self, in_, *args, **kwargs):
        self.__set_sbx_state__(json.load(in_), *args, **kwargs)


class _ExecutorOps(object):
    def __init__(self, runner):
        self.runner = runner

    def update_repr_state(self):
        self.runner._update_repr_state()

    def stop_waiting(self, stop_id):
        self.runner._graph_executor._stop_waiting(stop_id)

    def del_working(self, job):
        pass

    def add_working(self, job):
        pass

    def set_successfull_state(self):
        r = self.runner
        with r._lock:
            r._finished = True
            r._job_finished.notify()

    def set_errored_state(self):
        r = self.runner
        with r._lock:
            r._failed = True
            r._finished = True
            r._job_finished.notify()

    def job_done_successfully(self, job_id):
        pass # TODO Notify rem_server for job_done_tag

    def create_job_runner(self, job):
        return rem.job.JobRunner(job)


class PacketRunner(object):
    def __init__(self, pck, directory):
        self._pck = pck
        self._directory = directory
        self._finished = False
        self._failed = False
        self._lock = threading.RLock()
        self._job_finished = threading.Condition(self._lock)
        self._proc_runner = None
        self._graph_executor = None

    def get_working_directory(self):
        return self._directory + '/work'

    #def run(self):
        #runner_srv = rem.subprocsrv.create_runner()
        #try:
            #self._do_run()
        #finally:
            #try:
                #runner_srv.stop()
            #except:
                #pass

    def _start_one_another_job(self):
        runner = self._graph_executor.get_job_to_run()
        t = ProfiledThread(target=runner.run)
        t.start()

    def run(self):
        self._proc_runner = rem.job.create_job_runner(None, None)

        self._graph_executor = \
            self._pck._create_job_graph_executor(self, self.get_working_directory())

        while not self._finished:
            with self._lock:
                while self._graph_executor.can_run_jobs_right_now():
                    self._start_one_another_job()

                self._job_finished.wait()

    def _calc_repr_state(self):
        if self._finished:
            return ReprState.ERROR if self.failed else ReprState.SUCCESSFULL
        return self._graph_executor.calc_repr_state()

# OPS for JobGraphExecutor's OPS
    def _update_repr_state(self):
        new_state = self._calc_repr_state()
        if new_state != self.new_state:
            self.state = new_state
            logging.info("new state %s" % new_state)

    def _stop_waiting(self, stop_id):
        with self.lock:
            self._graph_executor.stop_waiting(stop_id)

# OPS for rem.job.Job
    def start_process(self, args, kwargs):
        return self._proc_runner(*args, **kwargs)

    def send_job_long_execution_notification(self):
        raise NotImplementedError()

    def _create_job_file_handles(self):
        return self._graph_executor.create_job_file_handles(job)

    def on_job_done(self, runner):
        self._graph_executor.on_job_done(runner)

        with self.lock:
            self._graph_executor.apply_jobs_results()

