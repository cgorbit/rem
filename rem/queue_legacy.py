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

        self.working_jobs = set()

        self.packets_with_pending_jobs = PackSet.create()

        by_user_state = self.by_user_state = ByUserState()

        # XXX This is wrong, but all packets will be relocated later
        for sub in ['pending', 'working', 'worked', 'errored', 'suspended', 'waited']:
            by_user_state.__dict__[sub] = dikt.pop(sub)

        self.is_suspended = dikt.pop('isSuspended')
        self.working_limit = dikt.pop('workingLimit')

        self.successfull_lifetime = dikt.pop('success_lifetime')
        #self.errored_lifetime = dikt.pop('errored_lifetime')

        self.successfull_default_lifetime = dikt.pop('successForgetTm')
        self.errored_default_lifetime = dikt.pop('errorForgetTm')

        self.__class__ = LocalQueue
