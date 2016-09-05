from rem.queue import LocalQueue, ByUserState
from rem.scheduler import Scheduler
from rem.common import PackSet, TimedSet, PickableRLock, emptyset, Unpickable
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
