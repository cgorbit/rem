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

    def subject(self):
        return "[REM@{server_name}] {level} {pck_id} {pck_name}: {subject}".format(
            pck_name=self.pck.name,
            pck_id=self.pck.id,
            server_name=self.ctx.network_name,
            subject=self._get_subject(),
            level=self.ERROR_LEVEL,
        )


class PacketExecutionError(IMessageHelper):
    ERROR_LEVEL = 'ERROR'

    def __init__(self, ctx, pck):
        self.pck = pck
        self.ctx = ctx

    def _get_subject(self):
        return "packet recovering error" if self.pck.is_broken \
            else "packet execution error"

    def message(self):
        mbuf = cStringIO.StringIO()

        print >> mbuf, "Packet {pck_id}/{pck_name} has been aborted because of some error states".format(
            pck_id=self.pck.id,
            pck_name=self.pck.name,
        )
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
    ERROR_LEVEL = 'ERROR'

    def __init__(self, ctx, pck):
        self.pck = pck
        self.ctx = ctx

    def _get_subject(self):
        return "has been marked to delete by EMERGENCY situation"

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet {pck_id}/{pck_name} has been marked to delete by EMERGENCY situation".format(
            pck_id=self.pck.id,
            pck_name=self.pck.name,
        )
        p_state = self.pck.Status()
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
    ERROR_LEVEL = 'WARN'

    def __init__(self, ctx, pck, job):
        self.pck = pck
        self.job = job
        self.ctx = ctx

    def _get_subject(self):
        return "now working too long, job id: %s" % self.job.id

    def message(self):
        mbuf = cStringIO.StringIO()
        print >> mbuf, "Packet {pck_id}/{pck_name} has job working too long".format(
            pck_id=self.pck.id,
            pck_name=self.pck.name,
        )
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
    ERROR_LEVEL = 'INFO'

    def __init__(self, ctx, pck, tag_name, comment, will_reset):
        self.pck = pck
        self.ctx = ctx
        self.reason = comment
        self.tag_name = tag_name
        self.will_reset = will_reset

    def _get_subject(self):
        return "{inter} reset by {tag_name}".format(
            inter='will be' if self.will_reset else 'will not be',
            tag_name=self.tag_name
        )

    def message(self):
        mbuf = cStringIO.StringIO()
        print >>mbuf, "Tag '%s' was reset." % self.tag_name
        print >>mbuf, "Reset reason:", utf8ifunicode(self.reason)
        print >>mbuf
        print >>mbuf, "You packet {pck_id}/{pck_name} {action}".format(
            pck_id=self.pck.id,
            pck_name=self.pck.name,
            action='will be reset' if self.will_reset \
                else "will not be reset, because it's not resetable"
        )
        print >>mbuf
        self._outputExtendedPacketStatus(mbuf, self.pck.id, self.pck.Status())
        return mbuf.getvalue()


class PacketBecomesTooOld(IMessageHelper):
    ERROR_LEVEL = 'ERROR'

    def __init__(self, ctx, pck):
        self.pck = pck
        self.ctx = ctx

    def _get_subject(self):
        return "Packet too old"

    def message(self):
        mbuf = cStringIO.StringIO()
        print >>mbuf, "You packet {pck_id}/{pck_name} exists too long in {pck_state} state, so moving to ERROR".format(
            pck_id=self.pck.id,
            pck_name=self.pck.name,
            pck_state=self.pck._repr_state,
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

def FormatLongExecutionWarning(ctx, pck, job):
    return TooLongWorkingWarning(ctx, pck, job).make()

def FormatPacketBecomesTooOld(ctx, self):
    return PacketBecomesTooOld(ctx, self).make()
