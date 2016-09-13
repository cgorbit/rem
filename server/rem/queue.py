# -*- coding: utf-8 -*-
from __future__ import with_statement
import itertools
import time

from common import emptydict, TimedSet, PackSet, PickableRLock, Unpickable, value_or_None
from packet import LocalPacket, SandboxPacket, PacketState, \
                   NotWorkingStateError, NonDestroyingStateError, NonTooOldMarkableStateError
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

    # for debug
    def _list_all(self):
        return list(self.pending) \
             + list(self.workable) \
             + list(self.successfull) \
             + list(self.error) \
             + list(self.suspended) \
             + list(self.waiting)


class LocalOps(Unpickable(
                    working_jobs=emptydict,
                    packets_with_pending_jobs=PackSet.create,
                    working_limit=(int, 1),
                )):

    def __init__(self, parent):
        super(LocalOps, self).__init__()
        self.parent = parent

# TODO Better
    def copy(self):
        ret = type(self)(self.parent)
        ret.working_jobs = self.working_jobs.copy()
        ret.packets_with_pending_jobs = self.packets_with_pending_jobs.copy()
        ret.working_limit = self.working_limit
        return ret

    def set_working_limit(self, val):
        self.working_limit = val
        self.notify_has_pending_if_need()

    def notify_has_pending_if_need(self):
        if self.has_startable_jobs():
            self._notify_has_pending()

    def _notify_has_pending(self):
        sched = self.parent.scheduler
        if sched:
            sched._on_job_pending(self.parent)

    def on_job_get(self, pck, job):
        self.working_jobs[job] = pck

    def on_job_done(self, pck, job):
        self.working_jobs.pop(job, None)

    def on_packet_attach(self, pck):
        for job in pck._get_working_jobs():
            self.working_jobs[job] = pck

        if pck.has_pending_jobs():
            self.packets_with_pending_jobs.add(pck)

    def on_packet_detach(self, pck):
        for job in pck._get_working_jobs():
            self.working_jobs.pop(job, None)

        self.packets_with_pending_jobs.discard(pck)

    def update_pending_jobs_state(self, pck):
        if pck.has_pending_jobs():
            if pck not in self.packets_with_pending_jobs:
                self.packets_with_pending_jobs.add(pck)
                self.notify_has_pending_if_need()
        else:
            if pck in self.packets_with_pending_jobs:
                self.packets_with_pending_jobs.remove(pck)

    def has_startable_jobs(self):
        return bool(self.packets_with_pending_jobs) \
            and len(self.working_jobs) < self.working_limit

    def get_packet_to_run(self):
        return self.packets_with_pending_jobs.peak()[0]

    def do_relocate_packet(self, pck):
        pass


class SandboxOps(Unpickable(
                    working_packets=set,
                    pending_packets=PackSet.create,
                    working_limit=(int, 1),
                )):

    def __init__(self, parent):
        super(SandboxOps, self).__init__()
        self.parent = parent

# TODO Better
    def copy(self):
        ret = type(self)(self.parent)
        ret.working_packets = self.working_packets.copy()
        ret.pending_packets = self.pending_packets.copy()
        ret.working_limit = self.working_limit
        return ret

    def set_working_limit(self, val):
        self.working_limit = val
        self.notify_has_pending_if_need()

    def notify_has_pending_if_need(self):
        sched = self.parent.scheduler
        if self.pending_packets and sched:
            sched._on_packet_pending(self.parent) # XXX not under lock!

    def on_packet_attach(self, pck):
        pass

    def on_packet_detach(self, pck):
        self.working_packets.discard(pck)

    def update_pending_jobs_state(self, pck):
        pass

    def get_packet_to_run(self):
        return self.pending_packets.peak()[0]

    def has_startable_packets(self):
        return self.pending_packets \
            and len(self.working_packets) < self.working_limit

    def do_relocate_packet(self, pck):
        if pck.state == PacketState.PENDING:
            self.pending_packets.add(pck)
        else:
            self.pending_packets.discard(pck)

        if pck.state in PacketState.StatesWithNotEmptyExecutor:
            self.working_packets.add(pck)
        else:
            self.working_packets.discard(pck)


class CombinedQueue(Unpickable(
                       packets=set,
                       suspended_packets=TimedSet.create,
                       is_suspended=bool,
                       lock=PickableRLock,
                       #working_limit=(int, 1),
                       successfull_lifetime=(int, 0),
                       successfull_default_lifetime=int,
                       errored_lifetime=(int, 0),
                       errored_default_lifetime=int,
                       suspended_lifetime=(int, 0),
                       suspended_default_lifetime=value_or_None,
                       by_user_state=ByUserState,
                    )
                ):

    LEGACY_FILTER_NAMING = {
        'waited':   'waiting',
        'errored':  'error',
        'working':  'workable',
        'worked':   'successfull',
    }

    def __init__(self, name):
        super(CombinedQueue, self).__init__()
        self.name = name
        self.local_ops = LocalOps(self)
        self.sandbox_ops = SandboxOps(self)

    def __repr__(self):
        return '<Queue %s; total=%d; loc<work=%d; pend=%d; lim=%d> sbx<work=%d; pend=%d; lim=%d> at 0x%x>' % (
            self.name,
            len(self.packets),

            len(self.local_ops.working_jobs),
            len(self.local_ops.packets_with_pending_jobs),
            self.local_ops.working_limit,

            len(self.sandbox_ops.working_jobs),
            len(self.sandbox_ops.packets_with_pending_jobs),
            self.sandbox_ops.working_limit,

            id(self)
        )

    def __getstate__(self):
        with self.lock:
            sdict = getattr(super(CombinedQueue, self), "__getstate__", lambda: self.__dict__)()

            by_user_state = sdict['by_user_state']
            for qname, q in by_user_state.items():
                by_user_state[qname] = q.copy()

            sdict['local_ops'] = sdict['local_ops'].copy()
            sdict['sandbox_ops'] = sdict['sandbox_ops'].copy()

            return sdict

    def SetSuccessLifeTime(self, lifetime):
        self.successfull_lifetime = lifetime

    def SetErroredLifeTime(self, lifetime):
        self.errored_lifetime = lifetime

    def SetSuspendedLifeTime(self, lifetime):
        self.suspended_lifetime = lifetime

    def _on_packet_state_change(self, pck):
        self.relocate_packet(pck)

    def UpdateContext(self, context):
        self.successfull_default_lifetime = context.successfull_packet_lifetime
        self.errored_default_lifetime = context.errored_packet_lifetime
        self.suspended_default_lifetime = context.suspended_packet_lifetime
        self.scheduler = context.Scheduler

    def forgetOldItems(self):
        self._forget_queue_old_items(
            self.by_user_state.successfull,
            self.successfull_lifetime or self.successfull_default_lifetime,
            self._destroy_old_packet
        )

        self._forget_queue_old_items(
            self.by_user_state.error,
            self.errored_lifetime or self.errored_default_lifetime,
            self._destroy_old_packet
        )

        suspended_lifetime = self.suspended_lifetime or self.suspended_default_lifetime
        if suspended_lifetime:
            self._forget_queue_old_items(
                self.suspended_packets,
                suspended_lifetime,
                self._mark_packet_as_too_old
            )

    def _destroy_old_packet(self, pck):
        try:
            pck.destroy()
        except NonDestroyingStateError: # race
            pass

    def _mark_packet_as_too_old(self, pck):
        try:
            pck.mark_as_too_old()
        except NonTooOldMarkableStateError: # race
            pass

    def _forget_queue_old_items(self, queue, ttl, modify):
        threshold = time.time() - ttl

        while True:
            with self.lock:
                if not queue:
                    return

                pck, t = queue.peak()

                if t >= threshold:
                    return

            # XXX Don't use lock here to prevent deadlock
            modify(pck)

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

            if pck.state in PacketState.LimitedLifetimeSuspendedStates:
                self.suspended_packets.add(pck)
            else:
                self.suspended_packets.discard(pck)

            self._do_relocate_packet(pck)

        self.notify_has_pending_if_need(pck)

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
        self.notify_has_pending_if_need()

    def Suspend(self):
        self.is_suspended = True

    def Status(self):
        status = {name: len(queue) for name, queue in self.by_user_state.items()}

        legacy_naming = {
            'waiting': 'waited',
            'error': 'errored',
            'workable': 'working',
            'successfull': 'worked',
        }

        for new, old in legacy_naming.items():
            status[old] = status[new]

        status.update({
            "alive": self.is_alive(),
            "working-limit": self.working_limit,
            "success-lifetime": self.successfull_lifetime or self.successfull_default_lifetime,
            "error-lifetime": self.errored_lifetime or self.errored_default_lifetime,
            "suspended-lifetime": self.suspended_lifetime or self.suspended_default_lifetime,
        })

        return status

    def ChangeWorkingLimit(self, local_limit, sandbox_limit=None):
        local_limit = int(local_limit)
        if sandbox_limit is None:
            sandbox_limit = local_limit
        else:
            sandbox_limit = int(sandbox_limit)

        if local_limit < 1 or sandbox_limit < 1:
            raise ValueError()

        self.local_ops.set_working_limit(local_limit)
        self.sandbox_ops.set_working_limit(sandbox_limit)

    def Empty(self):
        return not self.packets

    def _get_pck_ops(self, pck):
        return self.local_ops if isinstance(pck, LocalPacket) else self.sandbox_ops

    # Common
    def _do_relocate_packet(self, pck):
        self._get_pck_ops(pck).do_relocate_packet(pck)

    def _on_packet_attach(self, pck):
        self._get_pck_ops(pck).on_packet_attach(pck)

    def _on_packet_detach(self, pck):
        self._get_pck_ops(pck).on_packet_detach(pck)

    #def relocate_packet(self, pck):
        #raise NotImplementedError()

    def update_pending_jobs_state(self, pck):
        with self.lock:
            self._get_pck_ops(pck).update_pending_jobs_state(pck)

    def notify_has_pending_if_need(self, pck=None):
        if pck:
            self._get_pck_ops(pck).notify_has_pending_if_need()
        else:
            self.local_ops.notify_has_pending_if_need()
            self.sandbox_ops.notify_has_pending_if_need()

    # Local
    def get_job_to_run(self):
        while True:
            with self.lock:
                if not self.has_startable_jobs():
                    return None

                pck = self.local_ops.get_packet_to_run()

            # .get_job_to_run not under lock to prevent deadlock
            try:
                job = pck.get_job_to_run()
            except NotWorkingStateError:
                logging.debug('NotWorkingStateError idling: %s' % pck)
                continue

            return job

    def has_startable_jobs(self):
        return self.is_alive() and self.local_ops.has_startable_jobs()

    def _on_job_done(self, pck, job):
        ops = self.local_ops
        with self.lock:
            ops.on_job_done(pck, job)
        ops.notify_has_pending_if_need()

    def _on_job_get(self, pck, job):
        with self.lock:
            self.local_ops.on_job_get(pck, job)

    # Sandbox
    def get_packet_to_run(self):
        with self.lock:
            if not self.has_startable_packets():
                return None
            return self.sandbox_ops.get_packet_to_run()

    def has_startable_packets(self):
        with self.lock:
            return self.is_alive() and self.sandbox_ops.has_startable_packets()


from rem.queue_legacy import Queue, LocalQueue, QueueBase, SandboxQueue
Queue
LocalQueue
SandboxQueue
