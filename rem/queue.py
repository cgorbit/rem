from __future__ import with_statement
import itertools
import time
import logging

from common import emptyset, TimedSet, PackSet, PickableRLock, Unpickable
from callbacks import CallbackHolder, ICallbackAcceptor
from packet import JobPacket, PacketCustomLogic, PacketState


class Queue(Unpickable(pending=PackSet.create,
                       worked=TimedSet.create,
                       errored=TimedSet.create,
                       suspended=set,
                       waited=set,
                       working=emptyset,
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
        PacketState.SUSPENDED: "suspended",
        PacketState.WORKABLE: "suspended",
        PacketState.PENDING: "pending",
        PacketState.ERROR: "errored",
        PacketState.SUCCESSFULL: "worked",
        PacketState.WAITING: "waited",
        PacketState.NONINITIALIZED: "noninitialized"
    }

    def __init__(self, name):
        super(Queue, self).__init__()
        self.name = name

    def __getstate__(self):
        sdict = getattr(super(Queue, self), "__getstate__", lambda: self.__dict__)()
        with self.lock:
            for q in self.VIEW_BY_ORDER:
                sdict[q] = sdict[q].copy()
        return sdict

    def SetSuccessLifeTime(self, lifetime):
        self.success_lifetime = lifetime

    def SetErroredLifeTime(self, lifetime):
        self.errored_lifetime = lifetime

    def OnJobGet(self, ref):
        with self.lock:
            self.working.add(ref)

    def OnJobDone(self, ref):
        with self.lock:
            self.working.discard(ref)
        if self.HasStartableJobs():
            self.FireEvent("task_pending")

    def OnChange(self, ref):
        if isinstance(ref, JobPacket):
            self.relocatePacket(ref)
            if ref.state == PacketState.WAITING:
                self.FireEvent("waiting_start", ref)

    def UpdateContext(self, context):
        self.successForgetTm = context.success_lifetime
        self.errorForgetTm = context.error_lifetime

    def forgetOldItems(self):
        self.forgetQueueOldItems(self.worked, self.success_lifetime or self.successForgetTm)
        self.forgetQueueOldItems(self.errored, self.errored_lifetime or self.errorForgetTm)

    def forgetQueueOldItems(self, queue, expectedLifetime):
        # XXX Don't use lock here to prevent deadlock
        barrierTm = time.time() - expectedLifetime
        while len(queue) > 0:
            # Race with RPC
            pck, tm = queue.peak()
            if tm < barrierTm:
                pck.RemoveAsOld()
            else:
                break

    def relocatePacket(self, pck):
        dest_queue_name = self.VIEW_BY_STATE.get(pck.state)
        dest_queue = getattr(self, dest_queue_name) if dest_queue_name else None

        with self.lock:
            if not(dest_queue and pck in dest_queue):
                logging.debug("queue %s\tmoving packet %s typed as %s", self.name, pck.name, pck.state)
                self.movePacket(pck, dest_queue)

    def _find_packet_queue(self, pck):
        ret = None
        for qname in self.VIEW_BY_ORDER:
            queue = getattr(self, qname, {})
            if pck in queue:
                if ret is not None:
                    logging.warning("packet %r is in several queues", pck)
                ret = queue
        return ret

    def movePacketOld(self, pck, dest_queue):
        src_queue = None
        for qname in self.VIEW_BY_ORDER:
            queue = getattr(self, qname, {})
            if pck in queue:
                if src_queue is not None:
                    logging.warning("packet %r is in several queues", pck)
                src_queue = queue
        if src_queue != dest_queue:
            with self.lock:
                if src_queue is not None:
                    src_queue.remove(pck)
                if dest_queue is not None:
                    dest_queue.add(pck)
            if pck.state == PacketState.PENDING:
                self.FireEvent("task_pending")

    def movePacket(self, pck, dst_queue):
        with self.lock:
            src_queue = self._find_packet_queue(pck)

            if src_queue == dst_queue:
                return

            if src_queue is not None:
                src_queue.remove(pck)

            if dst_queue is not None:
                dst_queue.add(pck)

        if pck.state == PacketState.PENDING:
            self.FireEvent("task_pending")

    #def OnPacketReinitRequest(self, code):
        #self.FireEvent('packet_reinit_request', code)

    #def OnPendingPacket(self, ref):
        #self.FireEvent("task_pending")

    def IsAlive(self):
        return not self.isSuspended

    def _attach_packet(self, pck):
        with self.lock:
            pck.AddCallbackListener(self) # XXX_LOCK
            self.working.update(pck._get_working_jobs()) # XXX_LOCK
        self.relocatePacket(pck)

    def _detach_packet(self, pck):
        with self.lock:
            if not self._find_packet_queue(pck):
                raise RuntimeError("packet %s is not in queue %s" % (pck.id, self.name))
            pck.DropCallbackListener(self) # XXX_LOCK
            self.working.difference_update(pck._get_working_jobs()) # XXX_LOCK
        self.movePacket(pck, None)

    def HasStartableJobs(self):
        with self.lock:
            return self.pending and len(self.working) < self.workingLimit and self.IsAlive()

    def Get(self, context):
        while True:
            with self.lock:
                if not self.HasStartableJobs():
                    return None

                pck, prior = self.pending.peak()

            # .GetJobToRun not under lock to prevent deadlock
            job = pck.GetJobToRun()

            if job == JobPacket.INCORRECT:
                continue

            return job

    def ListAllPackets(self):
        return itertools.chain(*(getattr(self, q) for q in self.VIEW_BY_ORDER))

    def GetWorkingPackets(self):
        return set(job.packetRef for job in self.working)

    def FilterPackets(self, filter=None):
        filter = filter or "all"
        pf, parg = {"errored": (list, self.errored), "suspended": (list, self.suspended),
                    "pending": (list, self.pending), "worked": (list, self.worked),
                    "working": (Queue.GetWorkingPackets, self),
                    "waiting": (list, self.waited), "all": (Queue.ListAllPackets, self)}[filter]
        for pck in pf(parg):
            yield pck

    def ListPackets(self, filter=None, name_regex=None, prefix=None, last_modified=None):
        packets = []
        for pck in self.FilterPackets(filter):
            if name_regex and not name_regex.match(pck.name):
                continue
            if prefix and not pck.name.startswith(prefix):
                continue
            if last_modified and (not pck.History() or pck.History()[-1][1] < last_modified): # XXX_LOCK
                continue
            packets.append(pck)
        return packets

    def Resume(self):
        self.isSuspended = False
        self.FireEvent("task_pending")

    def Suspend(self):
        self.isSuspended = True

    def Status(self):
        return {"alive": self.IsAlive(), "pending": len(self.pending), "suspended": len(self.suspended),
                "errored": len(self.errored), "worked": len(self.worked),
                "waiting": len(self.waited), "working": len(self.working), "working-limit": self.workingLimit, 
                "success-lifetime": self.success_lifetime if self.success_lifetime > 0 else self.successForgetTm,
                "error-lifetime": self.errored_lifetime if self.errored_lifetime > 0 else self.errorForgetTm}

    def ChangeWorkingLimit(self, lmtValue):
        self.workingLimit = int(lmtValue)
        if self.HasStartableJobs():
            self.FireEvent('task_pending')

    def Empty(self):
        return not any(getattr(self, subq_name, None) for subq_name in self.VIEW_BY_ORDER)
