import rem.queue
from rem.queue import ByUserState
from rem.scheduler import Scheduler
from rem.common import PackSet, TimedSet, PickableRLock, emptyset, emptydict, Unpickable, value_or_None
from rem.callbacks import CallbackHolder, ICallbackAcceptor


class Queue(Unpickable(pending=PackSet.create,
                       working=emptyset,
                       worked=TimedSet.create,
                       errored=TimedSet.create,
                       suspended=set,
                       waited=set,
                       noninitialized=set,

                       isSuspended=bool,
                       lock=PickableRLock,
                       workingLimit=(int, 1),
                       errorForgetTm=int,
                       successForgetTm=int,
                       success_lifetime=(int, 0),
                       errored_lifetime=(int, 0)),
            CallbackHolder,
            ICallbackAcceptor):

    def convert_to_v2(self):
        dikt = self.__dict__

        self.scheduler = None
        schedulers = self._get_listeners_by_type(Scheduler)
        if schedulers:
            scheduler = schedulers[0]
            self.scheduler = scheduler
            self.DropCallbackListener(scheduler)

        # TODO noninitialized

        self.working_jobs = {}

        self.packets_with_pending_jobs = PackSet.create()

        by_user_state = self.by_user_state = ByUserState()

        # XXX This is wrong, but all packets will be relocated later
        for repl in ['pending',
                    ('working', 'workable'),
                    ('worked', 'successfull'),
                    ('errored', 'error'),
                    'suspended',
                    ('waited', 'waiting')
                ]:
            src, dst = repl if isinstance(repl, tuple) else (repl, repl)
            by_user_state.__dict__[dst] = dikt.pop(src)

        self.is_suspended = dikt.pop('isSuspended')
        self.working_limit = dikt.pop('workingLimit')

        self.successfull_lifetime = dikt.pop('success_lifetime')
        #self.errored_lifetime = dikt.pop('errored_lifetime')

        self.successfull_default_lifetime = dikt.pop('successForgetTm')
        self.errored_default_lifetime = dikt.pop('errorForgetTm')

        dikt.pop('callbacks', None)
        dikt.pop('nonpersistent_callbacks', None)

        self.__class__ = LocalQueue

    # for before convert checks
    def list_all_packets(self):
        import itertools
        view_by_order = "pending", "waited", "errored", "suspended", "worked", "noninitialized"
        return itertools.chain(*(getattr(self, sub) for sub in view_by_order))


class QueueBase(Unpickable(
                       packets=set,
                       suspended_packets=TimedSet.create,
                       is_suspended=bool,
                       lock=PickableRLock,
                       working_limit=(int, 1),
                       successfull_lifetime=(int, 0),
                       successfull_default_lifetime=int,
                       errored_lifetime=(int, 0),
                       errored_default_lifetime=int,
                       suspended_lifetime=(int, 0),
                       suspended_default_lifetime=value_or_None,

                       by_user_state=ByUserState,

                       # LocalQueue
                       working_jobs=emptydict,
                       packets_with_pending_jobs=PackSet.create,

                       # SandboxQueue
                       working_packets=set, # FIXME
                       pending_packets=PackSet.create,
                )):

    def convert_to_v3(self):
        by_user_state = self.by_user_state

        self.packets = set(
            itertools.chain(
                by_user_state.pending,
                by_user_state.workable,
                by_user_state.successfull,
                by_user_state.error,
                by_user_state.suspended,
                by_user_state.waiting,
            )
        )

def _convert_to_combined(self):
    sdict = self.__dict__

    working_limit = sdict.pop('working_limit')

    local = self.local_ops = rem.queue.LocalOps(self)
    local.working_limit = working_limit
    local.working_jobs = sdict.pop('working_jobs')
    local.packets_with_pending_jobs = sdict.pop('packets_with_pending_jobs')

    sbx = self.sandbox_ops = rem.queue.SandboxOps(self)
    sbx.working_limit = working_limit
    sbx.working_packets = sdict.pop('working_packets')
    sbx.pending_packets = sdict.pop('pending_packets')

    self.__class__ = rem.queue.CombinedQueue


class LocalQueue(QueueBase):
    def convert_to_v5(self):
        _convert_to_combined(self)

    def list_all_packets(self):
        return self.packets


class SandboxQueue(QueueBase):
    def convert_to_v3(self):
        super(SandboxQueue, self).convert_to_v3()
        self.pending_packets = PackSet.create(list(self.by_user_state.pending))

    def convert_to_v5(self):
        _convert_to_combined(self)

    def list_all_packets(self):
        return self.packets
