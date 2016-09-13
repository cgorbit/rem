import threading
import cProfile
import os
import time

from rem.osspec import set_thread_name, gettid as _gettid
from rem_logging import logger as logging


PROFILING_DIR = None


def __init():
    global PROFILING_DIR

    PROFILING_DIR = os.environ.get('REM_PROFILING_DIR', None)

    if PROFILING_DIR:
        PROFILING_DIR += '/%d' % int(time.time())
        os.mkdir(PROFILING_DIR)

__init()


def gettid():
    return _gettid() or 0


class NamedThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        name_prefix = kwargs.pop('name_prefix', None)
        if name_prefix is not None:
            kwargs['name'] = name_prefix + threading._newname('-%d')

        threading.Thread.__init__(self, *args, **kwargs)

    def _set_thread_name(self):
        set_thread_name('rem-' + self.name)
        logging.debug('NamedThread name for %d is %s' % (gettid(), self.name))

    def run(self):
        self._set_thread_name()
        self._do_run()

    def _do_run(self):
        try:
            f = getattr(self, '_run', threading.Thread.run.__get__(self))
            f()
        except:
            logging.exception('NamedThread %s [%d] failed' % (self.name, gettid()))
            raise


class ProfiledThread(NamedThread):
    def _run_profiled(self, func):
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(func)
        finally:
            profiler.dump_stats('%s/thread-%s-%d.profile' % (PROFILING_DIR, self.name, gettid()))

    def _do_run(self):
        func = super(ProfiledThread, self)._do_run

        if PROFILING_DIR is not None:
            self._run_profiled(func)
        else:
            func()
