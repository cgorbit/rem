import cStringIO
import time

def utf8ifunicode(str):
    return str.encode('utf-8') if isinstance(str, unicode) else str

class IMessageHelper(object):
    @classmethod
    def _outputJobResults(cls, out, job):
        if job.get("results"):
            print >>out, "\n".join("\tresult: %s" % v for v in job.get("results"))

    @classmethod
    def _outputExtendedPacketStatus(cls, out, pck_id, pck):
        print >>out, "Extended packet status:"
        print >>out, "packet id:", pck_id
        print >>out, "\n".join("%s: %s" % (k, v) for k, v in pck.iteritems() if k not in ("jobs", "history"))
        print >>out, "history:"
        for state, timestamp in pck.get("history", []):
            print >>out, "\t%s: %s" % (time.ctime(timestamp), state)
        for job in pck.get("jobs", []):
            print >>out, "--== subjob info ==--"
            for k in sorted(job):
                if k != "results":
                    print >>out, "\t%s: %s" % (k, utf8ifunicode(job.get(k, "N/A")))
            cls._outputJobResults(out, job)

    def make(self):
        return (self.subject(), self.message())

class PacketExecutionError(IMessageHelper):
    def __init__(self, ctx, pck):
        self.pck = pck
        self.ctx = ctx

    def subject(self):
# FIXME Use pck.state
        reason = "packet recovering error" if self.pck.is_broken \
            else "packet execution error"
        return "[REM@%(sname)s] Task '%(pname)s': %(reason)s" % {"pname": self.pck.name, "reason": reason,
                                                                 "sname": self.ctx.network_name}

    def message(self):
        mbuf = cStringIO.StringIO()

        print >> mbuf, "Packet '%(pname)s' has been aborted because of some error states" % {"pname": self.pck.name}
        p_state = self.pck.Status()
        jobs = p_state.get("jobs", [])
        for job in jobs:
            if job.get("state") == "errored":
                print >> mbuf, "--== failed subjob ==--"
                print >> mbuf, "\t%s: %s" % ('shell', utf8ifunicode(job.get('shell', "N/A")))
                self._outputJobResults(mbuf, job)
        print >> mbuf, "=" * 80
        self._outputExtendedPacketStatus(mbuf, self.pck.id, p_state)
        return mbuf.getvalue()


class EmergencyError(IMessageHelper):
    def __init__(self, ctx, pck):
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
    def __init__(self, ctx, job):
        self.pck = job.pck
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
        print >> mbuf, "job working time:", getattr(self.job, 'cached_working_time', None), 'sec'
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

    def __init__(self, ctx, pck, tag_name, comment, will_reset):
        self.pck = pck
        self.ctx = ctx
        self.reason = comment
        self.tag_name = tag_name
        self.will_reset = will_reset

    def subject(self):
        return "[REM@{host}] packet '{pck_name}' {inter} reset by {tag_name}".format(
            inter='will be' if self.will_reset else 'will not be',
            host=self.ctx.network_name,
            pck_name=self.pck.name,
            tag_name=self.tag_name
        )

    def message(self):
        mbuf = cStringIO.StringIO()
        print >>mbuf, "Tag '%s' was reset." % self.tag_name
        print >>mbuf, "Reset reason:", utf8ifunicode(self.reason)
        print >>mbuf
        print >>mbuf, "You packet {name} ({id}) {action}".format(
            id=self.pck.id,
            name=self.pck.name,
            action='will be reset' if self.will_reset \
                else "will not be reset, because it's not resetable"
        )
        print >>mbuf
        self._outputExtendedPacketStatus(mbuf, self.pck.id, self.pck.Status())
        return mbuf.getvalue()


def FormatPacketErrorStateMessage(ctx, pck):
    return PacketExecutionError(ctx, pck).make()

def FormatPacketEmergencyError(ctx, pck):
    return EmergencyError(ctx, pck).make()

def FormatPacketResetNotificationMessage(*args, **kwargs):
    return ResetNotification(*args, **kwargs).make()

def FormatLongExecutionWarning(ctx, job):
    return TooLongWorkingWarning(ctx, job).make()
