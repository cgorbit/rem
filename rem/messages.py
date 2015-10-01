import cStringIO
import time
from packet import PacketState, PacketFlag


class IMessageHelper(object): pass


def GetHelper(helper_cls, *args):
    if helper_cls:
        assert issubclass(helper_cls, IMessageHelper)
        return helper_cls(*args)


def GetHelperByPacketState(pck, ctx):
    if ctx and ctx.send_emails:
        if not ctx.send_emergency_emails and pck.CheckFlag(PacketFlag.RCVR_ERROR):
            return None
        helper_cls = {PacketState.ERROR: PACKET_ERROR}#, PacketState.SUCCESSFULL: PACKET_SUCCESS}
        return GetHelper(helper_cls.get(pck.state, None), pck, ctx)


def GetEmergencyHelper(pck, ctx):
    if ctx and ctx.send_emails and ctx.send_emergency_emails:
        return EmergencyError(pck, ctx)


def GetLongExecutionWarningHelper(job, ctx):
    if job and job.packetRef.notify_emails:
        return TooLongWorkingWarning(job, ctx)


def GetResetNotificationHelper(pck, ctx, message):
    if ctx:
        return ResetNotification(pck, ctx, message)


class PacketExecutionError(IMessageHelper):
    def __init__(self, pck, ctx):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
        reason = "packet recovering error" if self.pck.CheckFlag(PacketFlag.RCVR_ERROR) \
            else "packet execution error"
        return "[REM@%(sname)s] Task '%(pname)s': %(reason)s" % {"pname": self.pck.name, "reason": reason,
                                                                 "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()

        def appendJobItem(job, itemName):
            def get(key):
                val = job.get(key, "N/A")
                if isinstance(val, unicode):
                    val = val.encode('utf-8')
                return val
            print >> mbuf, "\t%s: %s" % (itemName, get(itemName))

        def appendJobResults(job):
            if job.get("results"):
                print >> mbuf, "\n".join("\tresult: %s" % v for v in job.get("results"))

        print >> mbuf, "Packet '%(pname)s' has been aborted because of some error states" % {"pname": self.pck.name}
        p_state = self.pck.Status()
        jobs = p_state.get("jobs", [])
        for job in jobs:
            if job.get("state") == "errored":
                print >> mbuf, "--== failed subjob ==--"
                appendJobItem(job, "shell")
                appendJobResults(job)
        print >> mbuf, "=" * 80
        print >> mbuf, "Extended packet status:"
        print >> mbuf, "packet id:", self.pck.id
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        for job in jobs:
            print >> mbuf, "--== subjob info ==--"
            for k in sorted(job):
                if k != "results":
                    appendJobItem(job, k)
            appendJobResults(job)
        return mbuf.getvalue()


class PacketExecutionSuccess(IMessageHelper):
    def __init__(self, pck, ctx):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
        return "[REM@%(sname)s] Task '%(pname)s': successfully executed." % {"pname": self.pck.name,
                                                                             "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet %(pname)s has successfully executed" % {"pname": self.pck.name}
        print >> mbuf, "Extended packet status:"
        print >> mbuf, "packet id:", self.pck.id
        p_state = self.pck.Status()
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        return mbuf.getvalue()


class EmergencyError(IMessageHelper):
    def __init__(self, pck, ctx):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
        return "[REM@%(sname)s] Task '%(pname)s'(%(pid)s) has been marked to delete by EMERGENCY situation" \
               % {"pname": self.pck.name, "pid": self.pck.id, "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet '%(pname)s' has been marked to delete by EMERGENCY situation" % {"pname": self.pck.name}
        print >> mbuf, "Extended packet status:"
        print >> mbuf, "packet id:", self.pck.id
        p_state = self.pck.Status()
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        for job in p_state.get("jobs", []):
            print >> mbuf, "--== subjob info ==--"
            print >> mbuf, "\n".join("\t%s: %s" % (k, v) for k, v in job.iteritems() if k != "results")
            if job.get("results"):
                print >> mbuf, "\n".join("\tresult: %s" % v for v in job.get("results"))
        return mbuf.getvalue()


class TooLongWorkingWarning(IMessageHelper):
    def __init__(self, job, ctx):
        self.pck = job.packetRef
        self.job = job
        self.ctx = ctx

    def subject(self):
        return "[REM@%(sname)s] Task '%(pname)s'(%(pid)s) now working too long, job id: %(jobid)s " \
               % {"pname": self.pck.name, "pid": self.pck.id, 'sname': self.ctx.network_name, 'jobid': self.job.id}

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet '%(pname)s' has job working too long" % {"pname": self.pck.name}
        print >> mbuf, "packet id:", self.pck.id
        print >> mbuf, "job id:", self.job.id
        print >> mbuf, "job wait limit:", self.job.notify_timeout
        print >> mbuf, "job working time:", self.job.cached_working_time, 'sec'
        print >> mbuf, "Extended packet status:"
        p_state = self.pck.Status()
        print >> mbuf, "\n".join("%s: %s" % (k, v) for k, v in p_state.iteritems() if k not in ("jobs", "history"))
        print >> mbuf, "history:"
        for state, timestamp in p_state.get("history", []):
            print >> mbuf, "\t%s: %s" % (time.ctime(timestamp), state)
        for job in p_state.get("jobs", []):
            print >> mbuf, "--== subjob info ==--"
            print >> mbuf, "\n".join("\t%s: %s" % (k, v) for k, v in job.iteritems() if k != "results")
            if job.get("results"):
                print >> mbuf, "\n".join("\tresult: %s" % v for v in job.get("results"))
        return mbuf.getvalue()


class ResetNotification(IMessageHelper):
    def __init__(self, pck, ctx, message):
        self.pck = pck
        self.ctx = ctx
        self.reason = message

    def subject(self):
        return "[REM@%(sname)s] packet '%(pname)s' need to be reset" \
            % {"pname": self.pck.name, "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()
        print >>mbuf, "some tags were reset"
        print >>mbuf, "may be your packet '%(pname)s' had to reset, but haven't" % {"pname": self.pck.name}
        if not self.pck.isResetable:
            print >>mbuf, "... because your packet doesn't allow automatical resets"
        print >>mbuf, "packet id:", self.pck.id
        print >>mbuf, "reset reason:", self.reason
        return mbuf.getvalue()


PACKET_ERROR = PacketExecutionError
PACKET_SUCCESS = PacketExecutionSuccess

