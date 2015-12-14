import threading
import cProfile
import os
import logging
import time

from rem.osspec

PROFILING_DIR = None
HAS_PRCTL = False
try:
    import prctl
    def set_thread_name(name):
        prctl.set_name(name)
    HAS_PRCTL = True
except ImportError:
    def set_thread_name(name):
        pass

def __init():
    global PROFILING_DIR

    PROFILING_DIR = os.environ.get('PROFILING_DIR', None)

    if PROFILING_DIR:
        PROFILING_DIR += '/%d' % int(time.time())
        os.mkdir(PROFILING_DIR)

__init()

def gettid():
    return rem.osspec.gettid() or 0

class ProfiledThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        name_prefix = kwargs.pop('name_prefix', None)
        if name_prefix is not None:
            kwargs['name'] = name_prefix + threading._newname('-%d')

        threading.Thread.__init__(self, *args, **kwargs)

    def _run_profiled(self, func):
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(func)
        finally:
            profiler.dump_stats('%s/thread-%s-%d.profile' % (PROFILING_DIR, self.name, gettid()))

    def run(self):
        set_thread_name('rem-' + self.name)

        if not HAS_PRCTL:
            logging.debug('ProfiledThread name for %d is %s' % (gettid(), self.name))

        func = getattr(self, '_run', threading.Thread.run.__get__(self))

        if PROFILING_DIR is not None:
            self._run_profiled(func)
        else:
            func()
