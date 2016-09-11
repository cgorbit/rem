# -*- coding: utf-8 -*-
import time
import threading

from common import Unpickable, safeStringEncode
import delayed_executor
from rem_logging import logger as logging
import osspec

#                                     /--jobs_to_retry<--\
#                                     |                  |
#                                     |                  |
#                                     |                  |
# parent_to_childs -> child_to_parents -> jobs_to_run -> active_jobs_cache
#                     ^                                  |
#                     |                                  |- failed_jobs
#                     |                                  |
#                     |                                  `- succeed_jobs
#                     |                                          |
#                     \-----------------------------------------/


def always(ctor):
    def always(*args):
        return ctor()
    return always


class GraphState(object):
    PENDING_JOBS = 0b00000001
    WORKING      = 0b00000010
    TIME_WAIT    = 0b00000100
    SUSPENDED    = 0b00001000
    ERROR        = 0b00010000
    SUCCESSFULL  = 0b00100000
    CANCELLED    = 0b01000000

    _NAMES = {
        PENDING_JOBS: 'PENDING_JOBS',
        WORKING:      'WORKING',
        TIME_WAIT:    'TIME_WAIT',
        SUSPENDED:    'SUSPENDED',
        ERROR:        'ERROR',
        SUCCESSFULL:  'SUCCESSFULL',
        CANCELLED:    'CANCELLED',
    }

    @classmethod
    def str(cls, mask):
        ret = cls._NAMES.get(mask)
        if ret:
            return ret

        return '|'.join(
            name
                for val, name in cls._NAMES.items()
                    if mask & val
        )

class _ActiveJobs(object):
    def __init__(self):
        self._running = {} # From get_job_to_run moment
        self._results = []
        self._lock = threading.Lock()
        self._empty = threading.Condition(self._lock)

    def __nonzero__(self):
        return bool(len(self))

    # FIXME Non-obvious meaning?
    def __len__(self):
        with self._lock:
            return len(self._running) + len(self._results)

    def has_results(self):
        return bool(self._results)

    def has_running(self):
        return bool(self._running)

    def wait_running_empty(self):
        if not self._running:
            return

        with self._lock:
            while self._running:
                self._empty.wait()

    def add(self, runner):
        with self._lock:
            job_id = runner.job.id
            #logging.debug('_running[%d] = %s' % (job_id, runner.job))
            assert job_id not in self._running, "Job %d already in _running" % job_id
            self._running[job_id] = runner

    def on_done(self, runner):
        with self._lock:
            job_id = runner.job.id
            #logging.debug('_running.pop(%d)' % job_id)
            assert job_id in self._running, "No Job %d in _running" % job_id
            self._running.pop(job_id)
            self._results.append(runner)

            if not self._running:
                self._empty.notify_all()

    def terminate(self):
        for runner in self._running.values():
            runner.cancel()

    def pop_result(self):
        return self._results.pop(0)


def _build_deps_dict(jobs):
    graph = {}

    for job in jobs.values():
        graph.setdefault(job.id, [])

        for parent_id in job.parents:
            graph.setdefault(parent_id, []).append(job.id)

    return graph


class JobGraphExecutor(Unpickable(
                            jobs=dict,
                            parent_to_childs=dict,

                            #pck_id=str,

                            kill_all_jobs_on_error=(bool, True),

                            child_to_parents=dict,
                            succeed_jobs=set,
                            failed_jobs=set,
                            jobs_to_run=set,
                            jobs_to_retry=dict,
                            active_jobs_cache=always(set),

                            _clean_state=(bool, False), # False for loading old backups
                            _active_jobs=always(_ActiveJobs),
                            created_inputs=set,
                      ) ):
    def __init__(self, ops, pck_id, graph):
        super(JobGraphExecutor, self).__init__()
        self.jobs = graph.jobs
        self.parent_to_childs = _build_deps_dict(graph.jobs)
        self.kill_all_jobs_on_error = graph.kill_all_jobs_on_error
        self._clean_state = True
        self._ops = ops
        self._init_job_deps_graph()
        self.state = None

    def init(self):
        self._update_state()

    def _update_state(self):
        new = self._calc_state()
        if new == self.state:
            return

        #logging.debug('JOB_GRAPH new state: %s, %s' % (GraphState.str(new), self.__dict__))

        self.state = new
        self._ops.on_state_change()

    def __getstate__(self):
        sdict = self.__dict__.copy()

        if sdict['active_jobs_cache']:
            sdict['jobs_to_run'] = sdict['jobs_to_run'] | sdict['active_jobs_cache']
        sdict.pop('active_jobs_cache', None)

        sdict['jobs_to_retry'] = {
            stop_id: (job_id, None, deadline)
                for stop_id, (job_id, cancel, deadline) in sdict['jobs_to_retry'].items()
        }

        sdict.pop('_active_jobs', None)

        return sdict

    def _calc_state(self):
        has_active_jobs = bool(self.active_jobs_cache)

        if len(self.jobs) == len(self.succeed_jobs):
            return GraphState.SUCCESSFULL

        elif has_active_jobs:
            ret = GraphState.WORKING
            if not self.failed_jobs and self.jobs_to_run:
                ret |= GraphState.PENDING_JOBS
            return ret

        elif self.failed_jobs:
            return GraphState.ERROR

        elif self.jobs_to_run:
            return GraphState.PENDING_JOBS

        elif self.jobs_to_retry:
            return GraphState.TIME_WAIT

        else:
            raise AssertionError("Unreachable: %s" % self)

    def is_cancelling(self):
        return False

    def _register_stop_waiting(self, job_id, deadline):
# TODO Use ids from delayed_executor as in sandbox_remote_packet
        stop_id = None
        stop_waiting = lambda : self._ops.stop_waiting(stop_id) # TODO BACK_REFERENCE
        stop_id = id(stop_waiting)

        cancel = delayed_executor.schedule(stop_waiting, deadline)

        self.jobs_to_retry[stop_id] = (job_id, cancel, deadline)

    def _cancel_all_wait_stoppers(self):
        for job_id, cancel, deadline in self.jobs_to_retry.values():
            if cancel:
                cancel()
            self.jobs_to_run.add(job_id)

        self.jobs_to_retry.clear()

    def get_nearest_retry_deadline(self):
        if not self.jobs_to_retry:
            #raise ValueError("No jobs to retry")
            return None
        return min(deadline for job_id, cancel, deadline in self.jobs_to_retry.values())

    def get_succeeded_jobs(self):
        return list(self.succeed_jobs)

    def stop_waiting(self, stop_id):
        descr = self.jobs_to_retry.pop(stop_id, None)

        if descr is None: # cancelled
            return

        job_id = descr[0]

        self.jobs_to_run.add(job_id)

        self._update_state()

    def _format_io_filename(self, (direction, job_id)):
        return '%s/%s-%s' % (self._ops.get_io_directory(), direction, job_id)

    def _create_input(self, jid):
        job = self.jobs.get(jid)

        if jid is None:
            raise AssertionError("alien job input request")

        if len(job.inputs) == 0:
            return osspec.get_null_input()

        io_id = ('in', jid)

        if len(job.inputs) == 1:
            io_id = ('out', job.inputs[0])

        elif io_id in self.created_inputs:
            pass

        else:
            filename = self._format_io_filename(io_id)

            with open(filename, "w") as writer:
                for pid in job.inputs:
                    with open(self._format_io_filename(('out', pid)), "r") as reader:
                        writer.write(reader.read()) # TODO chunks

            self.created_inputs.add(io_id)

        return open(self._format_io_filename(io_id))

    def _create_output(self, jid, type):
        if jid not in self.jobs:
            raise AssertionError("alien job output request")
        return open(self._format_io_filename((type, jid)), 'w')

    # We can't use lock here (deadlock)
    # FIXME We can avoid locking here because call to _kill_jobs() in _release_place()
    # garantee that pck.directory will exists
    def create_job_file_handles(self, job):
        return (
            self._create_input(job.id),
            self._create_output(job.id, "out"),
            self._create_output(job.id, "err")
        )

    def get_global_error(self):
        return None

    def _apply_job_result(self, job, runner):
        if runner.returncode_robust == 0 and not runner.is_cancelled():
            self.succeed_jobs.add(job.id)

            self._ops.job_done_successfully(job.id)

            for nid in self.parent_to_childs[job.id]:
                self.child_to_parents[nid].remove(job.id)
                if not self.child_to_parents[nid]:
                    self.jobs_to_run.add(nid)

        elif job.tries < job.max_try_count:
            delay = job.retry_delay \
                or max(job.ERR_PENALTY_FACTOR ** job.tries, job.ERR_PENALTY_FACTOR)
            self._register_stop_waiting(job.id, time.time() + delay)

        else:
            self.failed_jobs.add(job.id)

            if self.kill_all_jobs_on_error:
                self._kill_jobs_drop_results()

        self._update_state()

    def get_working_jobs(self):
        return [self.jobs[jid] for jid in list(self.active_jobs_cache)]

    def _process_job_result(self, processor, runner):
        job = runner.job
        assert job.id in self.jobs

        logging.debug("%s done\t%s\t%s", job, job.shell, job.last_result())

        self.active_jobs_cache.discard(job.id)
        self._ops.del_working(job)

        if processor:
            processor(job, runner)

    def get_job_to_run(self):
        if not self._can_run_jobs_right_now(): # duplicate, see LocalPacket.get_job_to_run
            raise RuntimeError()

        jid = self.jobs_to_run.pop()
        job = self.jobs[jid]
        self.active_jobs_cache.add(jid)
        self._ops.add_working(job) # for queue
        self._clean_state = False

        runner = self._ops.create_job_runner(job)
        self._active_jobs.add(runner)

        self._update_state()

        return runner

    def _can_run_jobs_right_now(self):
        return bool(self.jobs_to_run and not self.failed_jobs)

    has_jobs_to_run = _can_run_jobs_right_now

    # Modify:   child_to_parents, jobs_to_run
    # Consider: jobs_to_retry, succeed_jobs
    def _init_job_deps_graph(self):
        def visit(startJID):
            st = [[startJID, 0]]
            discovered.add(startJID)

            while st:
                # num - number of neighbours discovered
                jid, num = st[-1]
                adj = self.parent_to_childs[jid]
                if num < len(adj):
                    st[-1][1] += 1
                    nid = adj[num]
                    self.child_to_parents[nid].add(jid)
                    if nid not in discovered:
                        discovered.add(nid)
                        st.append([nid, 0])
                    elif nid not in finished:
                        raise RuntimeError("job dependencies cycle exists")
                else:
                    st.pop()
                    finished.add(jid)

        #with self.lock:
        if True:
            assert not self._active_jobs and not self.active_jobs_cache, "Has active jobs"
            assert not self.failed_jobs, "Has failed jobs"

            discovered = set()
            finished = set()

            self.child_to_parents = {jid: set() for jid in self.jobs if jid not in self.succeed_jobs}

            for jid in self.jobs:
                if jid not in discovered and jid not in self.succeed_jobs:
                    visit(jid)

            jobs_to_retry = set(descr[0] for descr in self.jobs_to_retry.values())

            self.jobs_to_run = set(
                jid for jid in discovered
                    if not self.child_to_parents[jid] and jid not in jobs_to_retry
            )

    def _do_reset(self):
        self._kill_jobs_drop_results()
        assert not self.active_jobs_cache, "Has active jobs"

        self._cancel_all_wait_stoppers()

        for job in self.jobs.values():
            job.results = []
            job.tries = 0

        self.succeed_jobs.clear()
        self.failed_jobs.clear()
        self.jobs_to_retry.clear()

        self._init_job_deps_graph()
        self.created_inputs.clear()

        self._clean_state = True # FIXME

    def _produce_job_status(self, job):
        job_id = job.id

        state = "done" if job_id in self.succeed_jobs \
            else "working" if job_id in self.active_jobs_cache \
            else "pending" if job_id in self.jobs_to_run \
            else "errored" if job_id in self.failed_jobs \
            else "suspended"
            #else "waiting" if job_id in self.jobs_to_retry \

        wait_jobs = []
        if self.active_jobs_cache: # FIXME Why this check?
            wait_jobs = map(str, self.child_to_parents.get(job_id, []))

        parents = map(str, job.parents or [])
        pipe_parents = map(str, job.inputs or [])

        output_filename = None

        results = [safeStringEncode(str(res)) for res in job.results]

        return dict(
            id=str(job.id),
            shell=job.shell,
            desc=job.description,
            state=state,
            results=results,
            parents=parents,
            pipe_parents=pipe_parents,
            output_filename=output_filename,
            wait_jobs=wait_jobs,
        )

    def produce_detailed_status(self):
        return [self._produce_job_status(job) for job in self.jobs.values()]

    def _reset_tries(self):
        self.jobs_to_run.update(self.failed_jobs)
        self.failed_jobs.clear()
        self._cancel_all_wait_stoppers()
        for job in self.jobs.values():
            job.tries = 0

    def reset_tries(self):
        self._reset_tries()
        self._update_state()

    def reset(self):
        if not self._clean_state:
            self._do_reset()
        self._update_state()

    def cancel(self):
        self._kill_jobs_drop_results()
        self._update_state()

    #reset = cancel

    def is_null(self):
        return not self.has_running_jobs()

    def on_job_done(self, runner):
        self._active_jobs.on_done(runner)

    def _process_jobs_results(self, processor):
        active = self._active_jobs

        while active.has_results():
            runner = active.pop_result()
            try:
                self._process_job_result(processor, runner)
            except Exception:
                logging.exception("Failed to process job result for %s" % runner.job)

    def apply_jobs_results(self):
        self._process_jobs_results(self._apply_job_result)

    def _drop_jobs_results(self):
        def revert_leaf(job, runner):
            self.jobs_to_run.add(job.id)
        self._process_jobs_results(revert_leaf)

    def _kill_jobs(self):
        active = self._active_jobs

        active.terminate()
        active.wait_running_empty()

    def _kill_jobs_drop_results(self):
        self._kill_jobs()
        self._drop_jobs_results()

    def has_running_jobs(self):
        return bool(self.active_jobs_cache)

    def recover_after_backup_loading(self):
        self.jobs_to_run.update(self.active_jobs_cache)
        self.active_jobs_cache.clear()
        #self.failed_jobs.clear() # FIXME Don't remember. Legacy behaviour?
        #self._init_job_deps_graph() # FIXME Why I comment this?

    def vivify_jobs_waiting_stoppers(self):
        jobs_to_retry, self.jobs_to_retry = self.jobs_to_retry, {}
        #for job_id, deadline in jobs_to_retry.items():
            #self._register_stop_waiting(job_id, deadline)
        for stop_id, (job_id, cancel, deadline) in jobs_to_retry.items():
            assert cancel is None
            self._register_stop_waiting(job_id, deadline)

    def get_worker_state(self):
        return self.state
