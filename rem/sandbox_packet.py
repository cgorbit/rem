import sys
import time
import json
import threading

import rem.job
import rem.job_graph
from rem.profile import ProfiledThread
#import rem.packet # XXX don't import
import rem.subprocsrv
from rem.packet_common import ReprState
from rem_logging import logger as logging

#class Resources(object):
    #def __init__(self):
        #self.added_files = [] # (id, digest, filename)
        #self.python = None
        #self.packet_executor = None


class _ExecutorOps(object):
    def __init__(self, pck):
        self.pck = pck

    def update_repr_state(self):
        self.pck._update_repr_state_if_need()

    def stop_waiting(self, stop_id):
        self.pck._stop_waiting(stop_id)

    def del_working(self, job):
        pass

    def add_working(self, job):
        pass

    def get_io_directory(self):
        return self.pck.get_io_directory()

    def set_successfull_state(self):
        r = self.pck
        r._finished = True
        r._job_finished.notify()
        logging.debug('+++ set_successfull_state')

    def set_errored_state(self):
        r = self.pck
        r._failed = True
        r._finished = True
        r._job_finished.notify()
        logging.debug('+++ set_errored_state')

    def job_done_successfully(self, job_id):
        pass # TODO Notify rem_server for job_done_tag
        self.pck._job_finished.notify()
        logging.debug('+++ job_done_successfully')

    def create_job_runner(self, job):
        return rem.job.JobRunner(self.pck, job) # brain damage

    def on_can_run_jobs(self):
        self.pck._job_finished.notify()


class Packet(object):
    def __init__(self, pck_id, jobs, jobs_graph, kill_all_jobs_on_error):
        self.id = pck_id
        self.name = '_TODO_packet_name_for_%s' % pck_id # TODO
        self.history = []
        self._finished = False
        self._failed = False
        #self._init_non_persistent()

        self._graph_executor = rem.job_graph.JobGraphExecutor(
            _ExecutorOps(self),
            jobs,
            jobs_graph,
            self.id,
            kill_all_jobs_on_error,
        )

        self.update_repr_state(ReprState.CREATED)

    def update_repr_state(self, state):
        self.repr_state = state
        self.history.append((state, time.time()))
        logging.info("new state %s" % state)

    def _init_non_persistent(self):
        self._lock = threading.RLock()
        self._job_finished = threading.Condition(self._lock)
        self._main_thread = None
        self._proc_runner = None

    def __getstate__(self):
        sdict = self.__dict__.copy()
        sdict.pop('_lock', None)
        sdict.pop('_job_finished', None)
        sdict.pop('_directory', None)
        sdict.pop('_main_thread', None)
        sdict.pop('_proc_runner', None)
        return sdict

    def __setstate__(self, sdict):
        self.__dict__.update(sdict)
        #self._init_non_persistent()

    def start(self, directory):
        self._directory = directory
        self._init_non_persistent()
        self._proc_runner = rem.job.create_job_runner(None, None)

        print >>sys.stderr, self._graph_executor.__dict__
        print >>sys.stderr, self._graph_executor.calc_repr_state()

        self._main_thread = ProfiledThread(target=self._main_loop, name_prefix='Packet')
        self._main_thread.start()

    def join(self):
        self._main_thread.join()

    def get_working_directory(self):
        return self._directory + '/work'

    def get_io_directory(self):
        return self._directory + '/io'

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
        logging.debug('+ Packet._start_one_another_job')
        job_runner = self._graph_executor.get_job_to_run()
        t = ProfiledThread(target=job_runner.run, name_prefix='Job')
        t.start()

    def _main_loop(self):
        logging.debug('+ Packet.run')

        while not self._finished:
            with self._lock:
                while self._graph_executor.can_run_jobs_right_now():
                    self._start_one_another_job()

                self._job_finished.wait()
        logging.debug('+ exiting Packet.run')

    def _calc_repr_state(self):
        if self._finished:
            return ReprState.ERROR if self.failed else ReprState.SUCCESSFULL
        return self._graph_executor.calc_repr_state()

# OPS for JobGraphExecutor's OPS
    def _update_repr_state_if_need(self):
        new_state = self._calc_repr_state()
        if new_state != self.repr_state:
            self.update_repr_state(new_state)

    def _stop_waiting(self, stop_id):
        with self._lock:
            self._graph_executor.stop_waiting(stop_id)

# OPS for rem.job.Job
    def start_process(self, *args, **kwargs):
        return self._proc_runner(*args, **kwargs)

    def send_job_long_execution_notification(self):
        raise NotImplementedError()

    def _create_job_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)

    def on_job_done(self, job_runner):
        self._graph_executor.on_job_done(job_runner)

        with self._lock:
            self._graph_executor.apply_jobs_results()

    def create_file_handles(self, job):
        return self._graph_executor.create_job_file_handles(job)




    #def __get_sbx_state__(self):
        #sdict = self.__dict__.copy()

        #sdict['jobs'] = {
            #id: job.__get_sbx_state__()
                #for id, job in sdict['jobs'].items()
        #}

        #return sdict

    #def __set_sbx_state__(self, sdict):
        #self.__dict__.clear()
        #self.__dict__.update(sdict)
        #self.jobs_graph = {
            #int(id): val
                #for id, val in self.jobs_graph.items()
        #}
        #self.jobs = {
            #int(id): rem.job.Job.from_sbx_state(job, self)
                #for id, job in self.jobs.items()
        #}
        #return self

    #@classmethod
    #def from_sbx_state(cls, sdict):
        #self = cls.__new__(cls)
        #self.__set_sbx_state__(sdict)
        #return self

    #def dump_json(self, out):
        #json.dump(self.__get_sbx_state__(), out, indent=3)

    #def dumps_json(self):
        #return json.dumps(self.__get_sbx_state__(), indent=3)

    #@classmethod
    #def loads_json(cls, s):
        #return cls.from_sbx_state(json.loads(s))

    #@classmethod
    #def load_json(cls, in_):
        #return cls.from_sbx_state(json.load(in_))


