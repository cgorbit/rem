# -*- coding: utf-8 -*-
from __future__ import with_statement
import itertools
import time

from common import emptyset, emptydict, TimedSet, PackSet, PickableRLock, Unpickable
from packet import LocalPacket, SandboxPacket, PacketCustomLogic, PacketState, NotWorkingStateError, NonDestroyingStateError
from rem_logging import logger as logging


class ByUserState(Unpickable(
                       pending=PackSet.create, # PackSet is for meaningfull order for user
                       workable=set,
                       successfull=TimedSet.create,
                       error=TimedSet.create,
                       suspended=set,
                       waiting=set,
                       #paused=set, TODO
                       #tags_wait=set,
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
                       packets=set,
                       is_suspended=bool,
                       lock=PickableRLock,
                       working_limit=(int, 1),
                       successfull_lifetime=(int, 0),
                       successfull_default_lifetime=int,
                       errored_lifetime=(int, 0),
                       errored_default_lifetime=int,

                       by_user_state=ByUserState,

                       # LocalQueue
                       working_jobs=emptydict,
                       packets_with_pending_jobs=PackSet.create,

                       # SandboxQueue
                       working_packets=set, # FIXME
                       pending_packets=PackSet.create,
                    )
                ):

    LEGACY_FILTER_NAMING = {
        'waited':   'waiting',
        'errored':  'error',
        'working':  'workable',
        'worked':   'successfull',
    }

    def __init__(self, name):
        super(QueueBase, self).__init__()
        self.name = name

    def __getstate__(self):
        sdict = getattr(super(QueueBase, self), "__getstate__", lambda: self.__dict__)()
        # FIXME copy without lock
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
        self._forget_queue_old_items(self.by_user_state.successfull, self.successfull_lifetime or self.successfull_default_lifetime)
        self._forget_queue_old_items(self.by_user_state.error, self.errored_lifetime or self.errored_default_lifetime)

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
        dest_queue_name = None if pck.state == PacketState.HISTORIED \
                                  or pck.state == PacketState.UNINITIALIZED \
                               else pck._repr_state.lower()

        dest_queue = self.by_user_state[dest_queue_name] if dest_queue_name else None

        with self.lock:
            if not(dest_queue and pck in dest_queue):
                logging.debug("queue %s\tmoving packet %s typed as %s", self.name, pck.name, pck._repr_state)
                self._update_by_user_state(pck, dest_queue)
            if pck.state == PacketState.HISTORIED:
                self.packets.discard(pck)
            self._do_relocate_packet(pck)

    def _find_packet_user_state_queue(self, pck):
        ret = None
        for qname, queue in self.by_user_state.items():
            if pck in queue:
                if ret is not None:
                    logging.warning("packet %r is in several queues", pck)
                ret = queue
        return ret

    def _update_by_user_state(self, pck, dst_queue):
        with self.lock:
            src_queue = self._find_packet_user_state_queue(pck)

            if src_queue == dst_queue:
                return

            if src_queue is not None:
                src_queue.remove(pck)

            if dst_queue is not None:
                dst_queue.add(pck)

    def is_alive(self):
        return not self.is_suspended

    def _attach_packet(self, pck):
        with self.lock:
            assert pck.queue is None

            if not isinstance(pck, self._PACKET_CLASS):
                raise RuntimeError("Packet %s is not %s, can't attach to %s" \
                    % (pck, self._PACKET_CLASS.__name__, self.name))

            pck.queue = self
            self.packets.add(pck)
            self._on_packet_attach(pck)

        self.relocate_packet(pck)

    def _detach_packet(self, pck):
        with self.lock:
            assert pck.queue is self, "packet %s is not in queue %s" % (pck.id, self.name)
            pck.queue = None
            self.packets.discard(pck)
            self._on_packet_detach(pck)
            self._update_by_user_state(pck, None)

    def list_all_packets(self):
        return self.packets

    def filter_packets(self, filter=None, name_regex=None, prefix=None,
                       min_mtime=None, max_mtime=None,
                       min_ctime=None, max_ctime=None,
                       user_labels=None):
        if filter == 'all':
            filter = None

        filter = self.LEGACY_FILTER_NAMING.get(filter, filter)

        with self.lock:
            packets = self.packets if filter is None else self.by_user_state[filter]
            packets = list(packets) # FIXME fast enough (better than do all under locking)

        if name_regex is None and prefix is None \
            and min_mtime is None and max_mtime is None \
            and min_ctime is None and max_ctime is None \
            and not user_labels:
            return packets

        matched = []

        for pck in packets:
            if name_regex and not name_regex.match(pck.name):
                continue

            if prefix and not pck.name.startswith(prefix):
                continue

            if min_mtime is not None or max_mtime is not None:
                mtime = pck.History()[-1][1]
                if min_mtime is not None and mtime < min_mtime \
                    or max_mtime is not None and mtime > max_mtime:
                    continue

            if min_ctime is not None or max_ctime is not None:
                ctime = pck.History()[0][1]
                if min_ctime is not None and ctime < min_ctime \
                    or max_ctime is not None and ctime > max_ctime:
                    continue

            if user_labels and (not pck.user_labels \
                                or not all(l in pck.user_labels for l in user_labels)):
                continue

            matched.append(pck)

        return matched

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
        #return not any(self.by_user_state[subq_name] for subq_name in self.VIEW_BY_ORDER)
        return not self.packets

    def convert_to_v3(self):
        by_user_state = self.by_user_state
        d = by_user_state.__dict__

        d['waiting'] = d.pop('waited')
        d['error'] = d.pop('errored')
        d['workable'] = d.pop('working')
        d['successfull'] = d.pop('worked')

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


class LocalQueue(QueueBase):
    _PACKET_CLASS = LocalPacket

    def __repr__(self):
        return '<LocalQueue %s; work=%d; pend=%d; lim=%s at 0x%x>' % (
            self.name,
            len(self.working_jobs),
            len(self.packets_with_pending_jobs),
            self.working_limit,
            id(self)
        )

    def _notify_has_pending_if_need(self):
        if self.has_startable_jobs() and self.scheduler:
            self.scheduler._on_job_pending(self)

    def _on_change_working_limit(self):
        self._notify_has_pending_if_need()

    def _on_job_get(self, pck, job):
        with self.lock:
            self.working_jobs[job] = pck

    def _on_resume(self):
        if self.scheduler:
            self.scheduler._on_job_pending(self)

    def _on_job_done(self, pck, job):
        with self.lock:
            self.working_jobs.pop(job, None)
        self._notify_has_pending_if_need()

    def relocate_packet(self, pck):
        QueueBase.relocate_packet(self, pck)
        self._notify_has_pending_if_need()

    def _on_packet_attach(self, pck):
        for job in pck._get_working_jobs():
            self.working_jobs[job] = pck

        if pck.has_pending_jobs():
            self.packets_with_pending_jobs.add(pck)

    def _on_packet_detach(self, pck):
        for job in pck._get_working_jobs():
            self.working_jobs.pop(job, None)

        self.packets_with_pending_jobs.discard(pck)

    def has_startable_jobs(self):
        with self.lock:
            return bool(self.packets_with_pending_jobs) \
                and len(self.working_jobs) < self.working_limit \
                and self.is_alive()

    def _get_working_packets(self):
        return set(self.working_jobs.values())

    def get_job_to_run(self):
        while True:
            with self.lock:
                if not self.has_startable_jobs():
                    return None

                pck, prior = self.packets_with_pending_jobs.peak()

            # .get_job_to_run not under lock to prevent deadlock
            try:
                job = pck.get_job_to_run()
            except NotWorkingStateError:
                logging.debug('NotWorkingStateError idling: %s' % pck)
                continue

            return job

    def update_pending_jobs_state(self, pck):
        with self.lock:
            if pck.has_pending_jobs():
                if pck not in self.packets_with_pending_jobs:
                    self.packets_with_pending_jobs.add(pck)
                    self._notify_has_pending_if_need()
            else:
                if pck in self.packets_with_pending_jobs:
                    self.packets_with_pending_jobs.remove(pck)

    def _do_relocate_packet(self, pck):
        pass


class SandboxQueue(QueueBase):
    _PACKET_CLASS = SandboxPacket

    def __repr__(self):
        return '<SandboxQueue %s; work=%d; pend=%d; lim=%s at 0x%x>' % (
            self.name,
            len(self.working_packets),
            len(self.pending_packets),
            self.working_limit,
            id(self)
        )

    def _notify_has_pending_if_need(self):
        if self.pending_packets and self.scheduler:
            self.scheduler._on_packet_pending(self) # XXX not under lock!

    def _on_change_working_limit(self):
        self._notify_has_pending_if_need()

    def _on_resume(self):
        self._notify_has_pending_if_need()

    def relocate_packet(self, pck):
        QueueBase.relocate_packet(self, pck)
        self._notify_has_pending_if_need()

    def _do_relocate_packet(self, pck):
        if pck.state == PacketState.PENDING:
            self.pending_packets.add(pck)
        else:
            self.pending_packets.discard(pck)

        if pck.state in PacketState.StatesWithNotEmptyExecutor:
            self.working_packets.add(pck)
        else:
            self.working_packets.discard(pck)

    def has_startable_packets(self):
        with self.lock:
            return self.pending_packets \
                and len(self.working_packets) < self.working_limit \
                and self.is_alive()

    def get_packet_to_run(self):
        with self.lock:
            if not self.has_startable_packets():
                return None

            return self.pending_packets.peak()[0]

    def _on_packet_attach(self, pck):
        pass

    def _on_packet_detach(self, pck):
        self.working_packets.discard(pck)

    def update_pending_jobs_state(self, pck):
        pass

    def convert_to_v3(self):
        super(SandboxQueue, self).convert_to_v3()
        self.pending_packets = PackSet.create(list(self.by_user_state.pending))


from rem.queue_legacy import Queue
