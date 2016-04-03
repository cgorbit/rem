import os
import sys
import types
import fcntl
import struct
import signal
import subprocess
import errno
import time

_EXIT_STATUS_IN_FILE = 200
_EXIT_NO_ARGV        = 201
_EXIT_NOT_SUPER_USER = 202
_EXIT_FAILED_REPORT  = 203
_EXIT_BAD_REPORT_FD  = 204

_RT_ERROR        = 1
_RT_CHILD_STATUS = 2

_PRC_TERM_SIGNAL = signal.SIGUSR1
_GRP_KILL_SIGNAL = signal.SIGUSR2

def _handle_status(status):
    if os.WIFSIGNALED(status):
        return -os.WTERMSIG(status)
    elif os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    else:
        raise RuntimeError("Unknown child exit status!")

def _set_cloexec(fd, state=True):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)

    if state:
        flags |= fcntl.FD_CLOEXEC
    else:
        flags &= ~fcntl.FD_CLOEXEC

    fcntl.fcntl(fd, fcntl.F_SETFD, flags)

def _unset_cloexec(fd):
    _set_cloexec(fd, False)

def _preexec_fn(report_fd):
    _unset_cloexec(report_fd)

    # Python has no sigprocmask (for portability), so use sigaction

    # Prevent race-condition in pgrpguard
    for sig in [_PRC_TERM_SIGNAL, _GRP_KILL_SIGNAL]:
        signal.signal(sig, signal.SIG_IGN)

class _Popen(subprocess.Popen):
    def __init__(self, report_fd, *args, **kwargs):
        self.__report_fd = report_fd
        subprocess.Popen.__init__(self, *args, **kwargs)

    def _close_fds(self, but):
        but0, but1 = sorted([but, self.__report_fd])
        os.closerange(3, but0)
        os.closerange(but0 + 1, but1)
        os.closerange(but1 + 1, subprocess.MAXFD)


class Error(Exception):
    pass

class WrapperUsageError(Error):
    pass

class WrapperNotFoundError(Error):
    pass
class WrapperStartError(Error):
    pass
class WrapperStartOSError(OSError, WrapperStartError):
    pass

class ProcessStartOSError(OSError, Error):
    pass

class WrapperProtocolError(Error):
    pass


def _interpret_exec_error(e):
    if isinstance(e, OSError):
        if e.errno == errno.ENOENT:
            return WrapperNotFoundError("Can't find wrapper binary")
        return WrapperStartOSError(e.errno, "Failed to run wrapper binary: %s" % e.strerror)
    return WrapperStartError("Failed to run wrapper binary: %s" % e)


def _parse_report(str):
    offset = [0]

    def get_int():
        len = struct.calcsize('i')
        ret = struct.unpack('i', buffer(str, offset[0], len))
        offset[0] += len
        return ret[0]

    def get_str():
        len = get_int()
        ret = struct.unpack('%ds' % len, buffer(str, offset[0], len))
        offset[0] += len
        return ret[0]

    type = get_int()

    if type == _RT_ERROR:
        get_int()
        errno = get_int()
        msg = get_str()
        ret = ProcessStartOSError(errno, "%s: %s" % (msg, os.strerror(errno)))

    elif type == _RT_CHILD_STATUS:
        ret = _handle_status(get_int())

    else:
        raise WrapperProtocolError("Unknown report type %d" % type)

    if offset[0] != len(str):
        raise WrapperProtocolError('Extra data in wrapper report')

    return ret


def _real_status_from_report(report_fd, wrapper_status, wrapper_filename):
    try:
        with os.fdopen(report_fd) as in_:
            report_str = in_.read()
    except Exception as e:
        return WrapperProtocolError("Failed to read report: %s" % e)

    if wrapper_status < 0:
        return WrapperProtocolError("Wrapper was terminated by %s signal" % -wrapper_status)

    elif wrapper_status == _EXIT_NO_ARGV:
        return WrapperUsageError("No enough arguments for %s" % wrapper_filename)

    elif wrapper_status == _EXIT_BAD_REPORT_FD:
        return WrapperUsageError("Bad report fd for %s" % wrapper_filename)

    elif wrapper_status == _EXIT_NOT_SUPER_USER:
        return WrapperUsageError("No set-uid root on %s" % wrapper_filename)

    elif wrapper_status == _EXIT_STATUS_IN_FILE:
        try:
            return _parse_report(report_str)
        except Exception as e:
            return WrapperProtocolError("Failed to parse report from %s: %s" % (wrapper_filename, e))

    elif wrapper_status == _EXIT_FAILED_REPORT:
        return WrapperProtocolError("%s failed to write report" % wrapper_filename)

    else:
        return wrapper_status


class ProcessGroupGuard(object):
    def __init__(self, argv, *args, **kwargs):
        if isinstance(argv, types.StringTypes):
            argv = [argv]
        else:
            argv = list(argv)

        if not argv:
            raise ValueError("No command to executer")

        if 'preexec_fn' in kwargs:
            raise ValueError('preexec_fn in arguments')

        wrapper_binary = kwargs.pop('wrapper_binary', 'pgrpguard')
        self._wrapper_filename = wrapper_binary

        report_pipe = os.pipe()
        _set_cloexec(report_pipe[0])
        _set_cloexec(report_pipe[1])

        kwargs['preexec_fn'] = lambda : _preexec_fn(report_pipe[1])

        self._result = None

        try:
            self._proc = _Popen(
                report_pipe[1],
                [self._wrapper_filename, str(report_pipe[1])] + argv,
                *args,
                **kwargs
            )

        except Exception:
            try:
                t, e, tb = sys.exc_info()
                e = _interpret_exec_error(e)
                raise type(e), e, tb
            finally:
                for fd in report_pipe:
                    try:
                        os.close(fd)
                    except:
                        pass
        else:
            os.close(report_pipe[1])

        self._report_fd = report_pipe[0]
        self.pid = self._proc.pid

    def _handle_status(self, wrapper_status):
        self._result = _real_status_from_report(self._report_fd, wrapper_status, self._wrapper_filename)
        self._report_fd = None

    @property
    def stdin(self):
        return self._proc.stdin

    @property
    def stdout(self):
        return self._proc.stdout

    @property
    def stderr(self):
        return self._proc.stderr

    def poll(self):
        return self._poll(self._proc.poll)

    def wait(self):
        return self._poll(self._proc.wait)

    @property
    def returncode(self):
        if isinstance(self._result, Exception):
            raise self._result
        return self._result

    def _poll(self, poll):
        if self._result is None:
            status = poll()

            if status is not None:
                self._handle_status(status)

        return self.returncode

    def communicate(self, input=None):
        ret = self._proc.communicate(input)
        self.wait()
        return ret

    def send_term_to_process(self):
        os.kill(self.pid, _PRC_TERM_SIGNAL)

    def send_kill_to_group(self):
        os.kill(self.pid, _GRP_KILL_SIGNAL)
