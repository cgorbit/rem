import os
import sys
import time
import signal
import subprocess
import threading

import subprocsrv
import subprocsrv_fallback
import pgrpguard

from common import check_process_call, check_process_retcode
from rem_logging import logger as logging

_inf = float('inf')
_MAX_WAIT_DELAY = 2.0


def _wait(f, timeout=None, deadline=None):
    if timeout is not None:
        deadline = time.time() + timeout

    delay = 0.005

    while True:
        res = f()
        if res is not None:
            return res

        if deadline is not None:
            remaining = deadline - time.time()
            if remaining <= 0.0:
                break
        else:
            remaining = _inf

        delay = min(delay * 2, remaining, _MAX_WAIT_DELAY)
        time.sleep(delay)

    return None


class ProcessProxyBase(object):
    BEFORE_KILL_DELAY = 1.0

    def __init__(self):
        # Lock for waitpid(2) call
        self._lock = threading.Lock() # no need in fork_locking.Lock
        self._signal_was_sent = False

    def was_signal_sent(self):
        return self._signal_was_sent

    def _waited(self):
        return self._impl.returncode is not None

    @property
    # May throw
    def returncode(self):
        return self._impl.returncode

    def wait(self, timeout=None, deadline=None):
        return _wait(self.poll, timeout, deadline)

    def poll(self):
        with self._lock:
            return self._impl.poll()


def _get_process_state(pid):
    with open('/proc/%d/status' % pid) as in_:
        for line in in_:
            if line.startswith('State:'):
                return line.rstrip('\n').split('\t')[1][0]


class ProcessProxy(ProcessProxyBase):

    # If process terminated by itself, we will not send SIGKILL to group
    # in contrast to pgrpguard

    def __init__(self, *args, **kwargs):
        ProcessProxyBase.__init__(self)
        kwargs['preexec_fn'] = os.setpgrp
        self._impl = subprocess.Popen(*args, **kwargs)

    def _send_term_to_process(self):
        self._signal_was_sent = True
        os.kill(self._impl.pid, signal.SIGTERM)

    def _send_kill_to_group(self):
        self._signal_was_sent = True
        os.killpg(self._impl.pid, signal.SIGKILL)

    def terminate(self):
        with self._lock:
            if self._waited():
                return

            self._send_term_to_process()

            pid = self._impl.pid
            def poll_zombie():
                return True if _get_process_state(pid) == 'Z' else None

            _wait(poll_zombie, timeout=self.BEFORE_KILL_DELAY)

            self._send_kill_to_group()


class ProcessGroupGuardProxy(ProcessProxyBase):

    # pgrpguard will send SIGKILL to group in any case

    def __init__(self, *args, **kwargs):
        ProcessProxyBase.__init__(self)
        self._impl = pgrpguard.ProcessGroupGuard(*args, **kwargs)

    def _send_term_to_process(self):
        self._signal_was_sent = True
        self._impl.send_term_to_process()

    def _send_kill_to_group(self):
        self._signal_was_sent = True
        self._impl.send_kill_to_group()

    def terminate(self):
        with self._lock:
            if self._waited():
                return
            self._send_term_to_process()

        if self.wait(self.BEFORE_KILL_DELAY):
            return

        with self._lock:
            if self._waited():
                return
            self._send_kill_to_group()


class SubprocsrvProcessProxy(object):
    BEFORE_KILL_DELAY = ProcessProxyBase.BEFORE_KILL_DELAY

    def __init__(self, runner, argv, stdin=None, stdout=None, stderr=None,
                 setpgrp=False, cwd=None, shell=False, use_pgrpguard=False):

        self._signal_was_sent = False

        if stdin:
            stdin = stdin.name
        if stdout:
            stdout = stdout.name
        if stderr:
            stderr = stderr.name

        self._impl = runner.Popen(argv, stdin, stdout, stderr, setpgrp, cwd,
                                      shell, use_pgrpguard)

    @staticmethod
    def _send_signal_safe_inspect_result(f):
        if not f.is_success():
            logging.warning("send_signal_safe failed: %s" % f.get_exception())

    def _send_signal_safe(self, sig, group):
        self._impl.send_signal_safe(sig, group) \
            .subscribe(self._send_signal_safe_inspect_result)

    def terminate(self):
        if self._impl.is_terminated():
            return

        # TODO _impl.send_signal_safe().get() gives better approximation
        self._signal_was_sent = True

        self._send_signal_safe(signal.SIGTERM, False)

        if self._impl.wait_no_throw(self.BEFORE_KILL_DELAY):
            return

        self._send_signal_safe(signal.SIGKILL, True)

    def was_signal_sent(self):
        return self._signal_was_sent

    @property
    # May throw
    def returncode(self):
        return self._impl.returncode

    def wait(self, timeout=None, deadline=None):
        return self._impl.wait(timeout, deadline)

    def poll(self):
        return self._impl.poll()
