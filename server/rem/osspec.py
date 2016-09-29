#OS specific functions
#Linux/FreeBSD implementation
from __future__ import with_statement
import os
import signal
import stat
import sys
import time

try:
    from _fork_locking import gettid
except ImportError:
    def gettid():
        return None


def is_pid_alive(pid):
    return os.path.isdir(os.path.join("/proc", str(pid)))


KILL_TICK = 0.001


def terminate(pid):
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(KILL_TICK)
        if is_pid_alive(pid):
            os.killpg(pid, signal.SIGKILL)
    except OSError:
        pass


def get_null_input():
    return open("/dev/null", "r")


def get_null_output():
    return open("/dev/null", "w")


def create_symlink(src, dst, reallocate=True):
    if reallocate and os.path.islink(dst):
        os.unlink(dst)
    return os.symlink(src, dst)


def set_common_executable(path):
    mode = os.stat(path)[0] | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    os.chmod(path, mode)


def set_common_readable(path):
    mode = os.stat(path)[0] | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH
    os.chmod(path, mode)


def get_shell_location(_cache=[]):
    if not _cache:
        _cache += [path for path in ("/bin/bash", "/usr/local/bin/bash", "/bin/sh") if os.access(path, os.X_OK)]
    return _cache[0]


try:
    from library.python.thread import set_thread_name
except ImportError:
    try:
        from prctl import set_name as set_thread_name
    except ImportError:
        def set_thread_name(name):
            pass

def set_process_cmdline(proc_title):
    """Sets custom title to current process
        Requires installed python-prctl module - http://pythonhosted.org/python-prctl/
    """
    try:
        import prctl
        prctl.set_proctitle(proc_title)
        return True
    except (ImportError, AttributeError):
        return False


def repr_term_status(status):
    return 'exit(%d)' % os.WEXITSTATUS(status) if os.WIFEXITED(status) \
      else 'kill(%d)' % os.WTERMSIG(status)


def set_oom_adj(adj):
    with open('/proc/self/oom_adj', 'w') as out:
        out.write(str(adj))


def get_oom_adj():
    with open('/proc/self/oom_score_adj') as io:
        return int(io.read().rstrip())
