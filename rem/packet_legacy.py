

# For loading legacy backups
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
                           # + dont_run_new_jobs
                          ),
                CallbackHolder,
                ICallbackAcceptor
               ):

    class PacketFlag:
        USER_SUSPEND = 0b0001
        RCVR_ERROR   = 0b0010

    StateMap = {
        ReprState.CREATED:          ImplState.UNINITIALIZED,
        ReprState.NONINITIALIZED:   ImplState.ACTIVE,
        ReprState.WORKABLE:         ImplState.ACTIVE,
        ReprState.PENDING:          ImplState.ACTIVE,
        #ReprState.SUSPENDED:
        ReprState.WAITING:          ImplState.ACTIVE,
        ReprState.ERROR:            ImplState.ERROR,
        ReprState.SUCCESSFULL:      ImplState.SUCCESSFULL,
        ReprState.HISTORIED:        ImplState.HISTORIED,
    }

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("JobPacket constructor is private")

    def convert_to_v2(self):
        pckd = self.__dict__

        self.failed_jobs = set()
        if self.state == ReprState.ERROR:
# FIXME pop may throw
            self.failed_jobs.add(self.leafs.pop())

        self.jobs_to_retry = {}
        if self.state == ReprState.WAITING:
# FIXME pop may throw
            self.jobs_to_retry[1] = (self.leafs.pop(), None, self.waitingDeadline)
        pckd.pop('waitingDeadline', None)

        self.done_tag = pckd.pop('done_indicator')
        #self.jobs_graph = pckd.pop('edges')
        self.succeed_jobs = pckd.pop('done')
        self.jobs_to_run = pckd.pop('leafs')
        self.job_done_tag = pckd.pop('job_done_indicator')
        self.all_dep_tags = pckd.pop('allTags')
        self.wait_dep_tags = pckd.pop('waitTags')
        self.bin_links = pckd.pop('binLinks')
        self.is_resetable = pckd.pop('isResetable')
        self.active_jobs_cache = pckd.pop('as_in_queue_working')

        self.dont_run_new_jobs = bool(self.flags & self.PacketFlag.USER_SUSPEND)
        has_recovery_error = bool(self.flags & self.PacketFlag.RCVR_ERROR)
        pckd.pop('flags')

        if self.state == ReprState.SUSPENDED:
            self._impl_state = ImplState.WAIT_TAGS if self.wait_dep_tags else ImplState.ACTIVE
        else:
            self._impl_state = self.StateMap[self.state]

        if self._impl_state == ImplState.ERROR and has_recovery_error:
            self._impl_state = ImplState.BROKEN
