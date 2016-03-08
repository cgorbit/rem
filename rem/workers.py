import copy
import time
import Queue as StdQueue

import osspec
from callbacks import CallbackHolder
from rem.profile import ProfiledThread
from rem_logging import logger as logging

class KillableWorker(ProfiledThread):
    def __init__(self, name_prefix='Worker'):
        super(KillableWorker, self).__init__(name_prefix=name_prefix)
        self.killed = False
        self.tickTime = self.TICK_PERIOD

    def do(self):
        pass

    def _run(self):
        while not self.IsKilled():
            try:
                self.do()
            except Exception, e:
                logging.exception("worker\tjob execution error %s", e)
            time.sleep(self.tickTime)

    def IsKilled(self):
        return self.killed

    def Kill(self):
        self.killed = True


class ThreadJobWorker(KillableWorker):
    TICK_PERIOD = 0.0

    def __init__(self, scheduler):
        super(ThreadJobWorker, self).__init__(name_prefix='JobWrk')
        self.scheduler = scheduler
        self.suspended = False
        self.job = None

    def do(self):
        if not self.IsSuspended() and self.scheduler.alive:
            try:
                self.job = self.scheduler.Get()
                if self.job:
                    self.job.run()
            finally:
                if self.job:
                    #if hasattr(self.job, 'packetRef'):
                        #logging.debug('ThreadJobWorker done_with %s from %s' % (self.job, self.job.packetRef))
                    self.job = None

    def IsSuspended(self):
        return self.suspended

    def Kill(self):
        super(ThreadJobWorker, self).Kill()

        job = self.job
        if job:
            job.cancel()

    def Resume(self):
        self.suspended = False

    def Suspend(self):
        self.suspended = True


class XMLRPCWorker(KillableWorker):
    TICK_PERIOD = 0.001
    WAIT_PERIOD = 1

    def __init__(self, requests, func):
        super(XMLRPCWorker, self).__init__(name_prefix='RPCWrk')
        self.requests = requests
        self.func = func

    def do(self):
        try:
            obj = self.requests.get(True, self.WAIT_PERIOD)
        except StdQueue.Empty:
            return
        else:
            self.func(*obj)


# Check proper base classes constructors are called after changes to this
class TimeTicker(KillableWorker, CallbackHolder):
    TICK_PERIOD = 1.0

    def __init__(self):
        CallbackHolder.__init__(self)
        KillableWorker.__init__(self, name_prefix='TimeTicker')

    def do(self):
        self.FireEvent("tick")
