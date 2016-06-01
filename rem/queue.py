from __future__ import with_statement
import itertools
import time

from common import emptyset, TimedSet, PackSet, PickableRLock, Unpickable
from callbacks import CallbackHolder, ICallbackAcceptor
from packet import LocalPacket, SandboxPacket, PacketCustomLogic, PacketState, NotWorkingStateError
from rem_logging import logger as logging


class ByUserState(Unpickable(
                       pending=PackSet.create,
                       working=set,
                       worked=TimedSet.create,
                       errored=TimedSet.create,
                       suspended=set,
                       waiting=set,
                       #noninitialized=set,
                 )):

    def items(self):
        return self.__dict__.items()

    def values(self):
        return self.__dict__.values()

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, val):
        self.__dict__[key] = val


class QueueBase(Unpickable(
                       by_user_state=ByUserState,

                    # LocalQueue # TODO move
                       working_jobs=emptyset,
                       packets_with_pending_jobs=PackSet.create,

                    # SandboxQueue # TODO move
                       working_packets=emptyset,
                       #_pending_packets <=> by_user_state['pending']

                       is_suspended=bool,
                       lock=PickableRLock,
                       working_limit=(int, 1),
                       successfull_lifetime=(int, 0),
                       successfull_default_lifetime=int,
                       errored_lifetime=(int, 0),
                       errored_default_lifetime=int,
                    )
                ):

    VIEW_BY_ORDER = "pending", "waiting", "errored", "suspended", "working", "worked"

    VIEW_BY_STATE = {
        PacketState.PAUSED:    "suspended",
        PacketState.PAUSING:   "suspended",
        PacketState.TAGS_WAIT: "suspended",

        # pending used to run SandboxPacket's
        PacketState.PENDING:                 "pending",
        PacketState.PREV_EXECUTOR_STOP_WAIT: "suspended", # FIXME
        PacketState.SHARING_FILES: "suspended", # FIXME

        PacketState.RUNNING:    "working",
        PacketState.DESTROYING: "working",

        PacketState.SUCCESSFULL: "worked",

        PacketState.TIME_WAIT: "waiting",

        PacketState.ERROR:  "errored",
        PacketState.BROKEN: "errored",

        PacketState.UNINITIALIZED: None, # stored in ShortStorage
        PacketState.HISTORIED:     None,
    }

# suspended
#     PAUSED or PAUSING or TAGS_WAIT

# pending
#     Old
#     PacketState.PENDING or PacketState.RUNNING and pck.has_pending_jobs()

#     New
#     PacketState.PENDING
#     PacketState.PREV_EXECUTOR_STOP_WAIT # FIXME

# working
#     PacketState.RUNNING
#     PacketState.DESTROYING # FIXME

# worked
#     PacketState.SUCCESSFULL

# waiting
#     PacketState.TIME_WAIT

# errored
#     PacketState.ERROR or PacketState.BROKEN

# None
#     UNINITIALIZED

########################################################################

# packets_with_pending_jobs
#     pck.has_pending_jobs()

# working_jobs (for limiting)
#     on callbacks

########################################################################

# pending_packets
#     by_user_state['pending']

# working_packets (for limiting)
#     pck.graph.state & GraphState.WORKING

########################################################################

    def __init__(self, name):
        super(QueueBase, self).__init__()
        self.name = name

    def __getstate__(self):
        sdict = getattr(super(QueueBase, self), "__getstate__", lambda: self.__dict__)()
# FIXME Bullshit: copy without lock
        with self.lock:
            by_user_state = sdict['by_user_state']
            for qname, q in by_user_state.items():
                by_user_state[qname] = q.copy()
        return sdict

    def SetSuccessLifeTime(self, lifetime):
        self.successfull_lifetime = lifetime

    def SetErroredLifeTime(self, lifetime):
        self.errored_lifetime = lifetime

    def _on_packet_state_change(self, pck):
        self.relocate_packet(pck)

    def UpdateContext(self, context):
        self.successfull_default_lifetime = context.successfull_packet_lifetime
        self.errored_default_lifetime = context.errored_packet_lifetime
        self.scheduler = context.Scheduler

    def forgetOldItems(self):
        self._forget_queue_old_items(self.by_user_state.worked, self.successfull_lifetime or self.successfull_default_lifetime)
        self._forget_queue_old_items(self.by_user_state.errored, self.errored_lifetime or self.errored_default_lifetime)

    def _forget_queue_old_items(self, queue, ttl):
        threshold = time.time() - ttl

        while True:
            with self.lock:
                if not queue:
                    return

                pck, t = queue.peak()

                if t >= threshold:
                    return

            # XXX Don't use lock here to prevent deadlock
            try:
                pck.destroy()

            # May throw: race with RPC calls, that may change packet state
            except NonDestroyingStateError:
                pass

    def relocate_packet(self, pck):
        dest_queue_name = self.VIEW_BY_STATE[pck.state]
        dest_queue = self.by_user_state[dest_queue_name] if dest_queue_name else None

        with self.lock:
            if not(dest_queue and pck in dest_queue):
                logging.debug("queue %s\tmoving packet %s typed as %s", self.name, pck.name, pck._repr_state)
                self._move_packet(pck, dest_queue)

    def _find_packet_queue(self, pck):
        ret = None
        for qname, queue in self.by_user_state.items():
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
                self._on_before_remove(pck)
                src_queue.remove(pck)

            if dst_queue is not None:
                dst_queue.add(pck)
                self._on_after_add(pck)

    def is_alive(self):
        return not self.is_suspended

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
        return itertools.chain(*(self.by_user_state[qname] for qname in self.VIEW_BY_ORDER))

    def _filter_packets(self, filter=None):
        if filter is None:
            return self.ListAllPackets()

        return list(self.by_user_state[filter])

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
        self.is_suspended = False
        self._on_resume()

    def Suspend(self):
        self.is_suspended = True

    def Status(self):
        status = {name: len(queue) for name, queue in self.by_user_state.items()}

        status.update({
            "alive": self.is_alive(),
            "working-limit": self.working_limit,
            "success-lifetime": self.successfull_lifetime or self.successfull_default_lifetime,
            "error-lifetime": self.errored_lifetime or self.errored_default_lifetime
        })

        return status

    def ChangeWorkingLimit(self, lmtValue):
        self.working_limit = int(lmtValue)
        self._on_change_working_limit()

    def Empty(self):
        return not any(self.by_user_state[subq_name] for subq_name in self.VIEW_BY_ORDER)


class LocalQueue(QueueBase):
    _PACKET_CLASS = LocalPacket

    def _notify_has_pending_if_need(self):
        logging.debug('_notify_has_pending_if_need ... %s' % self.has_startable_jobs())
# FIXME Optimize
        if self.has_startable_jobs():
            self.scheduler._on_job_pending(self)

    def _on_change_working_limit(self):
        self._notify_has_pending_if_need()

    def _on_job_get(self, ref):
        with self.lock:
            self.working_jobs.add(ref)

    def _on_resume(self):
        self.scheduler._on_job_pending(self)

    def _on_job_done(self, ref):
        with self.lock:
            self.working_jobs.discard(ref)
        self._notify_has_pending_if_need()

    def relocate_packet(self, pck):
        QueueBase.relocate_packet(self, pck)
        self._notify_has_pending_if_need()

    def _on_packet_attach(self, pck):
        self.working_jobs.update(pck._get_working_jobs())

    def _on_packet_detach(self, pck):
        self.working_jobs.difference_update(pck._get_working_jobs())
# FIXME Optimize
        self.packets_with_pending_jobs.discard(pck)

    def has_startable_jobs(self):
        with self.lock:
            return bool(self.packets_with_pending_jobs) \
                and len(self.working_jobs) < self.working_limit \
                and self.is_alive()

    def _get_working_packets(self):
        return set(job.pck for job in self.working_jobs)

    #def _get_working_count(self):
        #return len(self.working_jobs)

    def get_job_to_run(self):
        while True:
            with self.lock:
                if not self.has_startable_jobs():
                    return None

                pck, prior = self.packets_with_pending_jobs.peak()

            # .get_job_to_run not under lock to prevent deadlock
            try:
                job = pck.get_job_to_run()
            except NotWorkingStateError: # because of race
                logging.debug('NotWorkingStateError idling')
                continue

            return job

    def _on_before_remove(self, pck):
        #if pck.has_pending_jobs():
    # TODO
        self.packets_with_pending_jobs.discard(pck)

    def _on_after_add(self, pck):
# FIXME Optimize
        if pck.has_pending_jobs():
            self.packets_with_pending_jobs.add(pck)


class SandboxQueue(QueueBase):
    _PACKET_CLASS = SandboxPacket

    def _notify_has_pending_if_need(self):
        if self._pending_packets:
            self.scheduler._on_packet_pending(self) # XXX not under lock!

    def _on_change_working_limit(self):
        self._notify_has_pending_if_need()

    def _on_resume(self):
        self._notify_has_pending_if_need()

    @property
    def _pending_packets(self):
        return self.by_user_state['pending']

    def relocate_packet(self, pck):
        QueueBase.relocate_packet(self, pck)
        self._notify_has_pending_if_need()

    def has_startable_packets(self):
        with self.lock:
            return self._pending_packets \
                and len(self.working_packets) < self.working_limit \
                and self.is_alive()

    def get_packet_to_run(self):
        with self.lock:
            if not self.has_startable_packets():
                return None

            return self._pending_packets.peak()[0]

    def _on_packet_attach(self, pck):
        pass

    def _on_before_remove(self, pck):
        #else:
    # TODO
        self.working_packets.discard(pck)

    def _on_after_add(self, pck):
# FIXME Optimize
        if not pck._graph_executor.is_null():
            self.working_packets.add(pck)

    def _on_packet_detach(self, pck):
        self.working_packets.discard(pck)

    #def _get_working_count(self):
        #return len(self.working_packets)
