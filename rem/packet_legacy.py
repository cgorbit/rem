from rem.packet import ReprState, ImplState, LocalPacket, DUMMY_GRAPH_EXECUTOR, always
from rem.common import Unpickable, PickableRLock
from rem.callbacks import CallbackHolder, ICallbackAcceptor
from rem.queue import LocalQueue
from rem.queue_legacy import Queue as LegacyQueue

from rem_logging import logger as logging


_TAGS_AWAITED_STATES = frozenset([
    ReprState.WORKABLE,
    ReprState.PENDING,
    ReprState.ERROR,
    ReprState.SUCCESSFULL,
    ReprState.WAITING,
])


class JobPacket(Unpickable(lock=PickableRLock,
                           jobs=dict,
                           edges=dict, # jobs_graph
                           done=set, # succeed_jobs
                           leafs=set, # jobs_to_run
                           #_active_jobs=always(_ActiveJobs),
                           job_done_indicator=dict, # job_done_tag
                           waitingDeadline=int, # REMOVED
                           allTags=set, # all_dep_tags
                           waitTags=set, # wait_dep_tags
                           binLinks=dict, # bin_links
                           state=(str, ReprState.ERROR),
                           history=list,
                           notify_emails=list,
                           flags=int, # REMOVED
                           kill_all_jobs_on_error=(bool, True),
                           _clean_state=(bool, False), # False for loading old backups
                           isResetable=(bool, True), # is_resetable
                           notify_on_reset=(bool, False),
                           notify_on_skipped_reset=(bool, True),
                           directory=lambda *args: args[0] if args else None,
                           as_in_queue_working=always(set), # active_jobs_cache
                           # + jobs_to_retry
                           # + failed_jobs
                          ),
                CallbackHolder,
                ICallbackAcceptor
               ):

    class PacketFlag:
        USER_SUSPEND = 0b0001
        RCVR_ERROR   = 0b0010

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("JobPacket constructor is private")

    def convert_to_v2(self):
        for job in self.jobs.values():
            d = job.__dict__
            d.pop('packetRef', None)
            d.pop('callbacks', None)
            d.pop('nonpersistent_callbacks', None)
            job.pck_id = self.id

        pckd = self.__dict__

        state = pckd.pop('state')

        if state == ReprState.NONINITIALIZED:
            #self._recover_noninitialized(ctx)
            logging.error("Packet %s in NONINITIALIZED state" % self)

        self.do_not_run = bool(self.flags & self.PacketFlag.USER_SUSPEND)
        self.is_broken = bool(self.flags & self.PacketFlag.RCVR_ERROR)
        pckd.pop('flags')

        if state == ReprState.SUCCESSFULL and self.do_not_run:
            logging.warning("SUCCESSFULL and USER_SUSPEND in %s" % self.id)
            self.do_not_run = False

        pckd.pop('streams') # FIXME Overhead: will re-concat multi-deps
        pckd.pop('_active_jobs', None)

        pckd.pop('edges') # constant graph
        succeed_jobs = pckd.pop('done')
        jobs_to_run = pckd.pop('leafs')

        #active_jobs_cache = set()
        pckd.pop('as_in_queue_working')

        wait_job_deps = pckd.pop('waitJobs')

        def pop_failed_job():
            if not jobs_to_run:
                raise ValueError()

            for job_id in jobs_to_run:
                result = self.jobs[job_id].last_result()
                if not result:
                    continue
                if not result.IsSuccessfull():
                    jobs_to_run.remove(job_id)
                    return job_id

        jobs_to_retry = {}
        if state == ReprState.WAITING:
            if jobs_to_run:
                if self.waitingDeadline:
                    job_id = pop_failed_job() or jobs_to_run.pop()
                    jobs_to_retry[1] = (job_id, None, self.waitingDeadline)
                else:
                    logging.error("No waitingDeadline: %s" % self)
            else:
                logging.error("WAITING && !jobs_to_run: %s" % self)
        pckd.pop('waitingDeadline', None)

        failed_jobs = set()
        if state == ReprState.ERROR:
            job_id = pop_failed_job() if jobs_to_run else None
            if job_id:
                failed_jobs.add(job_id)
            elif not self.is_broken:
                logging.error("ERROR && !broken && !failed_jobs: %s" % self)

        working_jobs = {jid for jid, deps in wait_job_deps.items() if not deps} \
            - (succeed_jobs | jobs_to_run \
                | set(descr[0] for descr in jobs_to_retry.values()) \
                | failed_jobs)

        jobs_to_run |= working_jobs

        if working_jobs:
            logging.debug('working_jobs for %s in %s: %s' % (self.id, state, working_jobs))

        self.done_tag = pckd.pop('done_indicator')
        self.job_done_tag = pckd.pop('job_done_indicator')
        self.all_dep_tags = pckd.pop('allTags')
        self.bin_links = pckd.pop('binLinks')
        self.is_resetable = pckd.pop('isResetable')

        self.wait_dep_tags = pckd.pop('waitTags')
        # if we are in SUSPENDED (RCVR_ERROR or not) and len(self.wait_dep_tags)
        #   -- we will wait tags (in previous packet.py impl)
        self.tags_awaited = not self.wait_dep_tags or state in _TAGS_AWAITED_STATES

        clean_state = pckd.pop('_clean_state') # TODO apply to _graph_executor

        queues = self._get_listeners_by_type((LocalQueue, LegacyQueue)) # FIXME Select one type
        queue = queues[0] if queues else None
        self.queue = queue
        if queue:
            self.DropCallbackListener(queue)

        self.__class__ = LocalPacket

        self.files_modified = False
        self.resources_modified = False
        self.files_sharing = None
        self.shared_files_resource_id = None

        self.destroying = state == ReprState.HISTORIED
        self.sbx_files = {}

        self._repr_state = None
        self.state = None

        self.finish_status = True if state == ReprState.SUCCESSFULL else \
            (False if state == ReprState.ERROR and not self.is_broken else None)

        self._saved_jobs_status = None
        self._graph_executor = DUMMY_GRAPH_EXECUTOR

        self._repr_state = state # to avoid duplicates in pck.history

        if state == ReprState.SUCCESSFULL:
            g = self._create_job_graph_executor()
            self._saved_jobs_status = g.produce_detailed_status()
            del g

        elif self.queue:
            g = self._graph_executor = self._create_job_graph_executor()

            g.failed_jobs = failed_jobs
            g.succeed_jobs = succeed_jobs
            g.jobs_to_run = jobs_to_run
            g.jobs_to_retry = jobs_to_retry
            g.wait_job_deps = wait_job_deps

            g._clean_state = clean_state

            #g.init()
            g.state = g._calc_state()

        self.state = self._calc_state()
        self._update_repr_state()
        #self._update_state()

        if self._repr_state != state and not(state == ReprState.WORKABLE and self._repr_state == ReprState.PENDING):
            logging.warning("ReprState mismatch for %s: %s -> %s" % (self, state, self._repr_state))

