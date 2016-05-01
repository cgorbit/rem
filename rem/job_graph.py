# -*- coding: utf-8 -*-
import os
import time
import threading

from common import Unpickable, safeStringEncode, RpcUserError
import delayed_executor
from rem_logging import logger as logging
import osspec
from packet_common import ReprState


#                                     /--jobs_to_retry<--\
#                                     |                  |
#                                     |                  |
#                                     |                  |
# jobs_graph -> wait_job_deps -> jobs_to_run -> active_jobs_cache
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


class JobGraphExecutor(Unpickable(
                            jobs=dict,
                            jobs_graph=dict,

                            pck_id=str,

                            kill_all_jobs_on_error=(bool, True),

                            wait_job_deps=dict,
                            succeed_jobs=set,
                            failed_jobs=set,
                            jobs_to_run=set,
                            jobs_to_retry=dict,
                            active_jobs_cache=always(set),

                            _clean_state=(bool, False), # False for loading old backups
                            _active_jobs=always(_ActiveJobs),
                            created_inputs=set,

                            dont_run_new_jobs=bool, # FIXME Rename with meangin invertion?
                      ) ):
    def __init__(self, ops, pck_id, graph):
        super(JobGraphExecutor, self).__init__()
        self.jobs = graph.jobs
        self.jobs_graph = graph.build_deps_dict()
        self.kill_all_jobs_on_error = graph.kill_all_jobs_on_error
        self._clean_state = True
        self._ops = ops
        self._init_job_deps_graph()

    def __getstate__(self):
        sdict = self.__dict__.copy()

        sdict.pop('active_jobs_cache', None)

        sdict['jobs_to_retry'] = {
            #job_id: deadline for job_id, cancel, deadline in sdict['jobs_to_retry'].values()
            stop_id: (job_id, None, deadline)
                for stop_id, (job_id, cancel, deadline) in sdict['jobs_to_retry'].items()
        }

        sdict.pop('_active_jobs', None)

        return sdict

    def get_repr_state(self):
        has_active_jobs = bool(self.active_jobs_cache)

        if has_active_jobs:
            if self.dont_run_new_jobs or self.failed_jobs:
                return ReprState.WORKABLE
            elif self.jobs_to_run:
                return ReprState.PENDING
            else:
                return ReprState.WORKABLE

        elif self.failed_jobs:
            raise AssertionError("Unreachable") # ERROR reachable only on [not has_active_jobs]

        elif self.dont_run_new_jobs:
            return ReprState.SUSPENDED

        elif self.jobs_to_run:
            return ReprState.PENDING

        elif self.jobs_to_retry:
            return ReprState.WAITING

        else:
            raise AssertionError("Unreachable: %s" % self) # SUCCESSFULL

    def _register_stop_waiting(self, job_id, deadline):
        stop_id = None
        stop_waiting = lambda : self._ops.stop_waiting(stop_id) # TODO BACK_REFERENCE
        stop_id = id(stop_waiting)

        cancel = delayed_executor.add(stop_waiting, deadline)

        self.jobs_to_retry[stop_id] = (job_id, cancel, deadline)

    def _cancel_all_wait_stoppers(self):
        for job_id, cancel, deadline in self.jobs_to_retry.values():
            if cancel:
                cancel()
            self.jobs_to_run.add(job_id)

        self.jobs_to_retry.clear()

    def stop_waiting(self, stop_id):
        descr = self.jobs_to_retry.pop(stop_id, None)

        if descr is None: # cancelled
            return

        job_id = descr[0]

        had_to_run = bool(self.jobs_to_run)
        self.jobs_to_run.add(job_id)

        if not had_to_run:
            self._ops.update_repr_state() # TODO BACK_REFERENCE

        self._notify_can_run_jobs_if_need()

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

    def _notify_can_run_jobs_if_need(self):
        if self.can_run_jobs_right_now():
            self._ops.on_can_run_jobs()

    def _apply_job_result(self, job, runner):
        if runner.returncode_robust == 0 and not runner.is_cancelled():
            self.succeed_jobs.add(job.id)

            self._ops.job_done_successfully(job.id)

            for nid in self.jobs_graph[job.id]:
                self.wait_job_deps[nid].remove(job.id)
                if not self.wait_job_deps[nid]:
                    self.jobs_to_run.add(nid)

        elif job.tries < job.max_try_count:
            delay = job.retry_delay or job.ERR_PENALTY_FACTOR ** job.tries
            self._register_stop_waiting(job.id, time.time() + delay)

        else:
            self.failed_jobs.add(job.id)

            # Чтобы не вводить в заблуждение get_job_to_run
            #self._ops.update_repr_state() # XXX assert in _calc_repr_state

            if self.kill_all_jobs_on_error:
                self._kill_jobs_drop_results()

        self._notify_can_run_jobs_if_need()

    # TODO Check conditions again
        if len(self.succeed_jobs) == len(self.jobs):
            self._ops.set_successfull_state()
        # we can't start new jobs on _kill_jobs_drop_results=False
        elif not self.active_jobs_cache and self.failed_jobs:
            self._ops.set_errored_state()
        else:
            self._ops.update_repr_state()

    def get_working_jobs(self):
        return [self.jobs[jid] for jid in list(self.active_jobs_cache)]

    def _process_job_result(self, processor, runner):
        job = runner.job
        assert job.id in self.jobs

        logging.debug("job %s\tdone [%s]", job.shell, job.last_result())

        self.active_jobs_cache.discard(job.id)
        self._ops.del_working(job)

        if processor:
            processor(job, runner)

    def get_job_to_run(self):
        if not self.can_run_jobs_right_now(): # duplicate, see LocalPacket.get_job_to_run
            raise RuntimeError()

        jid = self.jobs_to_run.pop()
        job = self.jobs[jid]
        self.active_jobs_cache.add(jid)
        self._ops.add_working(job) # for queue
        self._clean_state = False

        runner = self._ops.create_job_runner(job)
        self._active_jobs.add(runner)

        return runner

    def can_run_jobs_right_now(self):
        return self.jobs_to_run and not (self.dont_run_new_jobs or self.failed_jobs)

    # add_job() -- это излишняя функциональность для JobGraphExecutor
    # TODO Move back to PacketBase, create JobGraphExecutor only on first ACTIVATE
    #def add_job(self, job):
        #for dep_id in job.parents:
            #if dep_id not in self.jobs:
                #raise RpcUserError(RuntimeError("No job with id = %s in packet %s" % (dep_id, self.pck_id)))

        #self.jobs[job.id] = job
        #self._add_job_to_graph(job)

    #def _add_job_to_graph(self, job):
        ## wait_job_deps[child] -> parents
        ## jobs_graph[parent]   -> children (constant between calls to Add)

        #parents = job.parents

        #self.jobs_graph[job.id] = []
        #for p in parents:
            #self.jobs_graph[p].append(job.id)

        #self.wait_job_deps[job.id] = [jid for jid in parents if jid not in self.succeed_jobs]

        #if not self.wait_job_deps[job.id]:
            #self.jobs_to_run.add(job.id)

    # Modify:   wait_job_deps, jobs_to_run
    # Consider: jobs_to_retry, succeed_jobs
    def _init_job_deps_graph(self):
        def visit(startJID):
            st = [[startJID, 0]]
            discovered.add(startJID)

            while st:
                # num - number of neighbours discovered
                jid, num = st[-1]
                adj = self.jobs_graph[jid]
                if num < len(adj):
                    st[-1][1] += 1
                    nid = adj[num]
                    self.wait_job_deps[nid].add(jid)
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

            self.wait_job_deps = {jid: set() for jid in self.jobs if jid not in self.succeed_jobs}

            for jid in self.jobs:
                if jid not in discovered and jid not in self.succeed_jobs:
                    visit(jid)

            jobs_to_retry = set(descr[0] for descr in self.jobs_to_retry.values())

            self.jobs_to_run = set(
                jid for jid in discovered
                    if not self.wait_job_deps[jid] and jid not in jobs_to_retry
            )

            #self._notify_can_run_jobs_if_need() # Don't call it here

    # jobs
    # jobs_graph        = edges
    # wait_job_deps     = waitJobs
    # jobs_to_run       = leafs
    # active_jobs_cache = as_in_queue_working
    # succeed_jobs      = done
    # + jobs_to_retry
    # + failed_jobs
    #
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
        self.dont_run_new_jobs = False

        #self._ops.recreate_working_directory() # FIXME

        self._clean_state = True # FIXME

    def produce_detailed_status(self):
        ret = []

        for jid, job in self.jobs.iteritems():
            result = job.last_result()
            results = []
            if result:
                results = [safeStringEncode(str(res)) for res in job.results]

            state = "done" if jid in self.succeed_jobs \
                else "working" if jid in self.active_jobs_cache \
                else "pending" if jid in self.jobs_to_run \
                else "errored" if result and not result.IsSuccessfull() \
                else "suspended"

            wait_jobs = []
            if self.active_jobs_cache:
                wait_jobs = map(str, self.wait_job_deps.get(jid, []))

            parents = map(str, job.parents or [])
            pipe_parents = map(str, job.inputs or [])

            output_filename = None

            ret.append(
                dict(id=str(job.id),
                        shell=job.shell,
                        desc=job.description,
                        state=state,
                        results=results,
                        parents=parents,
                        pipe_parents=pipe_parents,
                        output_filename=output_filename,
                        wait_jobs=wait_jobs,
                    )
            )

        return ret

    def disallow_to_run_jobs(self, kill_running):
        self.dont_run_new_jobs = True
        self._ops.update_repr_state() # maybe from PENDING to WORKABLE

        if kill_running:
            # FIXME In ideal world it's better to "apply" jobs that will be
            # finished racy just before kill(2)
            self._kill_jobs_drop_results()
            self._ops.update_repr_state() # maybe from PENDING to WORKABLE

        self._notify_can_run_jobs_if_need()

    def _reset_tries(self):
        self.jobs_to_run.update(self.failed_jobs)
        self.failed_jobs.clear()
        for job in self.jobs.values():
            job.tries = 0
        self._notify_can_run_jobs_if_need()

    def allow_to_run_jobs(self, reset_tries=False):
        if reset_tries:
            self._reset_tries()
        self.dont_run_new_jobs = False
        # FIXME NO_JOBS_SUSPENDED_MUST_NOT_BECOME_SUCESSFULL?
        self._ops.update_repr_state()
        self._notify_can_run_jobs_if_need()

# XXX
    def restart(self):
        if not self._clean_state:
            self._do_reset()
        self._notify_can_run_jobs_if_need()

    start = restart

    def stop(self): # for LocalPacket
        self._kill_jobs_drop_results()

    def cancel(self): # for SandboxPacket
        self._kill_jobs_drop_results()

raise AssertionError("How to use is_null for LocalPacket and on_job_graph_becomes_null")
    #def is_stopped(self):
        #return self.dont_run_new_jobs and not self.has_running_jobs()

    def is_null(self):
        return not self.has_running_jobs()

    def need_indefinite_time_to_reset(self):
        return False
# XXX

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
        if self.can_run_jobs_right_now():
            logging.error("_kill_jobs on %s when can_run_jobs_right_now() is True" % self.pck_id)

        active = self._active_jobs

        active.terminate()
        active.wait_running_empty()

    def _kill_jobs_drop_results(self):
        self._kill_jobs()
        self._drop_jobs_results()

    def has_running_jobs(self):
        return bool(self.active_jobs_cache)

    def recover_after_backup_loading(self):
        self.failed_jobs.clear() # FIXME
        self.active_jobs_cache.clear()
        #self._init_job_deps_graph()

    def vivify_jobs_waiting_stoppers(self):
        jobs_to_retry, self.jobs_to_retry = self.jobs_to_retry, {}
        #for job_id, deadline in jobs_to_retry.items():
            #self._register_stop_waiting(job_id, deadline)
        for stop_id, (job_id, cancel, deadline) in jobs_to_retry.items():
            assert cancel is None
            self._register_stop_waiting(job_id, deadline)
