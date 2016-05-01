from __future__ import with_statement
import itertools
import time

from common import emptyset, TimedSet, PackSet, PickableRLock, Unpickable
from callbacks import CallbackHolder, ICallbackAcceptor
from packet import LocalPacket, SandboxPacket, PacketCustomLogic, ReprState as PacketState, ImplState as PacketImplState, NotWorkingStateError
from rem_logging import logger as logging


class QueueBase(Unpickable(pending=PackSet.create,
                       worked=TimedSet.create,
                       errored=TimedSet.create,
                       suspended=set,
                       waited=set,
                       working_jobs=emptyset, # move to LocalQueue
                       isSuspended=bool,
                       noninitialized=set,
                       lock=PickableRLock,
                       errorForgetTm=int,
                       successForgetTm=int,
                       workingLimit=(int, 1),
                       success_lifetime=(int, 0),
                       errored_lifetime=(int, 0)),
            CallbackHolder,
            ICallbackAcceptor):

    VIEW_BY_ORDER = "pending", "waited", "errored", "suspended", "worked", "noninitialized"

    VIEW_BY_STATE = {
        PacketState.SUSPENDED: "suspended", # SUSPENDED packet may have running jobs
        PacketState.WORKABLE: "suspended",  # Wrong
        PacketState.PENDING: "pending",     # PENDING packet may have running jobs
        PacketState.ERROR: "errored",
        PacketState.SUCCESSFULL: "worked",
        PacketState.WAITING: "waited",
        PacketState.NONINITIALIZED: "noninitialized"
        # CREATED packets stored in ShortStorage
    }

    def __init__(self, name):
        super(QueueBase, self).__init__()
        self.name = name

    def __getstate__(self):
        sdict = getattr(super(QueueBase, self), "__getstate__", lambda: self.__dict__)()
        with self.lock:
            for q in self.VIEW_BY_ORDER:
                sdict[q] = sdict[q].copy()
        return sdict

    def SetSuccessLifeTime(self, lifetime):
        self.success_lifetime = lifetime

    def SetErroredLifeTime(self, lifetime):
        self.errored_lifetime = lifetime

    def _on_packet_state_change(self, pck):
        self.relocate_packet(pck)

    def UpdateContext(self, context):
        self.successForgetTm = context.success_lifetime
        self.errorForgetTm = context.error_lifetime
        self.scheduler = context.Scheduler

    def forgetOldItems(self):
        self._forget_queue_old_items(self.worked, self.success_lifetime or self.successForgetTm)
        self._forget_queue_old_items(self.errored, self.errored_lifetime or self.errorForgetTm)

    def _forget_queue_old_items(self, queue, ttl):
        threshold = time.time() - ttl

        old = []
        with self.lock:
            while queue:
                pck, t = queue.peak()

                if t >= threshold:
                    break

                old.append(pck)

        # XXX Don't use lock here to prevent deadlock
        for pck in old:
            pck.destroy() # May throw: race with RPC calls, that may change packet state

    def relocate_packet(self, pck):
        dest_queue_name = self.VIEW_BY_STATE.get(pck.state)
        dest_queue = getattr(self, dest_queue_name) if dest_queue_name else None

        with self.lock:
            if not(dest_queue and pck in dest_queue):
                logging.debug("queue %s\tmoving packet %s typed as %s", self.name, pck.name, pck.state)
                self._move_packet(pck, dest_queue)

    def _find_packet_queue(self, pck):
        ret = None
        for qname in self.VIEW_BY_ORDER:
            queue = getattr(self, qname, {})
            if pck in queue:
                if ret is not None:
                    logging.warning("packet %r is in several queues", pck)
                ret = queue
        return ret

    def _move_packet(self, pck, dst_queue):
        with self.lock:
            src_queue = self._find_packet_queue(pck)

            if src_queue == dst_queue:
                return

            if src_queue is not None:
                src_queue.remove(pck)

            if dst_queue is not None:
                dst_queue.add(pck)

    def IsAlive(self):
        return not self.isSuspended

    def _attach_packet(self, pck):
        with self.lock:
            assert pck.queue is None

            if not isinstance(pck, self._PACKET_CLASS):
                raise RuntimeError("Packet %s is not %s, can't attach to %s" \
                    % (pck, self._PACKET_CLASS.__name__, self.name))

            pck.queue = self
            self._on_packet_attach(pck)

        self.relocate_packet(pck)

    def _detach_packet(self, pck):
        with self.lock:
            assert pck.queue is self, "packet %s is not in queue %s" % (pck.id, self.name)
            pck.queue = None
            self._on_packet_detach(pck)
        self._move_packet(pck, None)

    def ListAllPackets(self):
        return itertools.chain(*(getattr(self, q) for q in self.VIEW_BY_ORDER))

    def _filter_packets(self, filter=None):
        pf, parg = {"errored": (list, self.errored),
                    "suspended": (list, self.suspended),
                    "pending": (list, self.pending),
                    "worked": (list, self.worked),
                    "working": (type(self)._get_working_packets, self),
                    "waiting": (list, self.waited),
                    None: (QueueBase.ListAllPackets, self)}[filter]
        for pck in pf(parg):
            yield pck

    def rpc_list_packets(self, filter=None, name_regex=None, prefix=None, last_modified=None):
        if filter == 'all':
            filter = None
        with self.lock:
            packets = []
            for pck in self._filter_packets(filter):
                if name_regex and not name_regex.match(pck.name):
                    continue
                if prefix and not pck.name.startswith(prefix):
                    continue
                if last_modified and (not pck.History() or pck.History()[-1][1] < last_modified):
                    continue
                packets.append(pck)
            return packets

    def Resume(self):
        self.isSuspended = False
        self._on_resume()

    def Suspend(self):
        self.isSuspended = True

    def Status(self):
        return {"alive": self.IsAlive(), "pending": len(self.pending), "suspended": len(self.suspended),
                "errored": len(self.errored), "worked": len(self.worked),
                "waiting": len(self.waited),
                "working": self._get_working_count(), # get working (job or packet) count
                "working-limit": self.workingLimit,
                "success-lifetime": self.success_lifetime if self.success_lifetime > 0 else self.successForgetTm,
                "error-lifetime": self.errored_lifetime if self.errored_lifetime > 0 else self.errorForgetTm}

    def ChangeWorkingLimit(self, lmtValue):
        self.workingLimit = int(lmtValue)
        self._on_change_working_limit()

    def Empty(self):
        return not any(getattr(self, subq_name, None) for subq_name in self.VIEW_BY_ORDER)


class LocalQueue(QueueBase):
    _PACKET_CLASS = LocalPacket

    def _on_change_working_limit(self):
        if self.has_startable_jobs():
            self.scheduler._on_job_pending(self)

    def _on_job_get(self, ref):
        with self.lock:
            self.working_jobs.add(ref)

    def _on_resume(self):
        self.scheduler._on_job_pending(self)

    def _on_job_done(self, ref):
        with self.lock:
            self.working_jobs.discard(ref)
        if self.has_startable_jobs():
            self.scheduler._on_job_pending(self)

    def relocate_packet(self, pck):
        QueueBase.relocate_packet(pck)

        if pck.state == PacketState.PENDING:
            self.scheduler._on_job_pending(self)

    def _on_packet_attach(self, pck):
        self.working_jobs.update(pck._get_working_jobs())

    def _on_packet_detach(self, pck):
        self.working_jobs.difference_update(pck._get_working_jobs())

    def has_startable_jobs(self):
        with self.lock:
            return self.pending and len(self.working_jobs) < self.workingLimit and self.IsAlive()

    def _get_working_packets(self):
        return set(job.pck for job in self.working_jobs)

    def _get_working_count(self):
        return len(self.working_jobs)

    def get_job_to_run(self):
        while True:
            with self.lock:
                if not self.has_startable_jobs():
                    return None

                pck, prior = self.pending.peak()

            # .get_job_to_run not under lock to prevent deadlock
            try:
                job = pck.get_job_to_run()
            except NotWorkingStateError: # because of race
                continue

            return job


class SandboxQueue(QueueBase):
    _PACKET_CLASS = SandboxPacket

    def _on_change_working_limit(self):
        pass
        #raise NotImplementedError()

    def _on_resume(self):
        pass
        #raise NotImplementedError()

    def relocate_packet(self, pck):
        QueueBase.relocate_packet(pck)

        if pck._impl_state == PacketImplState.PENDING: # TODO
            self.scheduler._on_job_pending(self)

# TODO FIXME
    def has_startable_packets(self):
        with self.lock:
            return self.really_pending \ # TODO rename
                and len(self.working_packets) < self.workingLimit \
                and self.IsAlive()

    def get_packet_to_run(self):
        while True:
            with self.lock:
                if not self.has_startable_packets():
                    return None

                return self.really_pending.peak()[0]

    def _on_packet_attach(self, pck):
        self.really_pending.add(pck)

    def _on_packet_detach(self, pck):
        self.really_pending.remove(pck)
#TODO
#TODO
#TODO
