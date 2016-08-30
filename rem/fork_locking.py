import threading
import os
import time
import sys
from sys import stderr
import signal
import errno
from collections import namedtuple
import traceback

__all__ = ["Lock", "RLock", "Condition", "LockWrapper", "RLockWrapper" "RunningChildInfo", "TerminatedChildInfo", "run_in_child"]

if 'DUMMY_FORK_LOCKING' not in os.environ:
    try:
        import _fork_locking
    except ImportError:
        import _dummy_fork_locking as _fork_locking
else:
    import _dummy_fork_locking as _fork_locking

try:
    gettid = _fork_locking.gettid
except AttributeError:
    def gettid():
        return None

try:
    set_fork_friendly_acquire_timeout = _fork_locking.set_fork_friendly_acquire_timeout
except AttributeError:
    pass

def acquire_lock(lock, blocking=True):
    if not blocking:
        raise RuntimeError("Non-blocking acquire not implemented")
    _fork_locking.acquire_lock()
    return lock.acquire()

def release_lock(lock):
    lock.release()
    _fork_locking.release_lock()

def acquire_restore_lock(lock, count_owner):
    _fork_locking.acquire_lock()
    return lock._acquire_restore(count_owner)

def release_save_lock(lock):
    ret = lock._release_save()
    _fork_locking.release_lock()
    return ret

def acquire_fork():
    _fork_locking.acquire_fork()

def release_fork():
    _fork_locking.release_fork()

class LockWrapperBase(object):
    __slots__ = ['_backend', '_name']

    def __init__(self, backend, name=None):
        self._backend = backend
        self._name = name or '__noname__'

    def acquire(self, blocking=True, label=None):
        acquire_lock(self._backend, blocking)

    def release(self):
        release_lock(self._backend)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()

class RLockWrapper(LockWrapperBase):
    __slots__ = []

    def _acquire_restore(self, count_owner):
        acquire_restore_lock(self._backend, count_owner)

    def _release_save(self):
        return release_save_lock(self._backend)

    def _is_owned(self):
        return self._backend._is_owned()

class LockWrapper(LockWrapperBase):
    __slots__ = []

    def _is_owned(self):
        if self._backend.acquire(0):
            self._backend.release()
            return False
        else:
            return True

def Lock(name=None):
    return LockWrapper(threading.Lock(), name)

def RLock(name=None):
    return RLockWrapper(threading.RLock(), name)

def Condition(lock, verbose=None):
    return threading.Condition(lock, verbose)

def _timed(func): # noexcept
    t0 = time.time()
    ret = func()
    return ret, time.time() - t0

def _dup_dev_null(file, flags):
    file_fd = file.fileno()
    dev_null = os.open('/dev/null', flags)
    os.dup2(dev_null, file_fd)
    os.close(dev_null)

RunningChildInfo = namedtuple('RunningChildInfo', ['pid', 'stderr', 'timings'])

def start_in_child(func, child_max_working_time=None):
    assert(child_max_working_time is None or int(child_max_working_time))

    _, acquire_time = _timed(acquire_fork)

    try:
        child_err_rd, child_err_wr = os.pipe()

        try:
            pid, fork_time = _timed(os.fork)
        except:
            t, v, tb = sys.exc_info()
            try:
                os.close(child_err_rd)
                os.close(child_err_wr)
            except:
                pass
            raise t, v, tb
    finally:
        release_fork()

    if not pid:
        try:
            os.close(child_err_rd)
            _dup_dev_null(sys.stdin, os.O_RDONLY)
            _dup_dev_null(sys.stdout, os.O_WRONLY)
            # stderr is character-buffered, so no buffer cleanup needed
            os.dup2(child_err_wr, sys.stderr.fileno())
            os.close(child_err_wr)

            if child_max_working_time is not None:
                signal.alarm(child_max_working_time)

            func()
        except Exception as e:
            try:
                traceback.print_exc()
            except:
                pass
            exit_code = 1
        else:
            exit_code = 0
        os._exit(exit_code)

    os.close(child_err_wr)

    return RunningChildInfo(
        pid=pid,
        stderr=os.fdopen(child_err_rd),
        timings={
            'acquire_time': acquire_time,
            'fork_time': fork_time
        }
    )

TerminatedChildInfo = namedtuple('TerminatedChildInfo', ['term_status', 'errors', 'timings'])

def run_in_child(func, child_max_working_time=None):
    child = start_in_child(func, child_max_working_time)

    with child.stderr:
        errors = child.stderr.read()

    while True:
        try:
            _, status = os.waitpid(child.pid, 0)
        except OSError as e:
            if e.errno != errno.EINTR:
                raise
        else:
            break

    return TerminatedChildInfo(status, errors, child.timings)
