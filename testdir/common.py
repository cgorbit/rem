import time
import logging
import os.path
import subprocess
import remclient
import random

__all__ = ["PrintPacketResults", "TestingQueue", "LmtTestQueue", "Config",
           "WaitForExecution", "WaitForStates", "WaitForExecutionList", "PrintCurrentWorkingJobs",
           "ServiceTemporaryShutdown", "RestartService", "RemServerWrapper"]


class SharedValue(object):
    def __init__(self, value=None):
        self.value = value

    def Get(self):
        return self.value


TestingQueue = SharedValue()
LmtTestQueue = SharedValue()
Config = SharedValue()


def _toPacketInfoIfNeed(pck):
    return pck.conn.PacketInfo(pck) if isinstance(pck, remclient.JobPacket) else pck

def WaitForExecution(pck, fin_states=("SUCCESSFULL", "ERROR"), timeout=1.0):
    while True:
        pck.update()
        cur_state = pck.state

        if cur_state in fin_states:
            break

        logging.info("packet state: %s", cur_state)
        time.sleep(timeout)

    return cur_state

def WaitForStates(some, fin_states=("SUCCESSFULL", "ERROR"), timeout=1.0):
    if isinstance(some, list):
        return WaitForExecutionList(map(_toPacketInfoIfNeed, some), fin_states, timeout)
    else:
        return WaitForExecution(_toPacketInfoIfNeed(some), fin_states, timeout)

def PrintPacketResults(pckInfo):
    for job in pckInfo.jobs:
        print job.shell, "\n".join(r.data for r in job.results)


def WaitForExecutionList(pckList, fin_states=("SUCCESSFULL", "ERROR"), timeout=1.0):
    while True:
        remclient.JobPacketInfo.multiupdate(pckList)
        waitPckCount = sum(1 for pck in pckList if pck.state not in fin_states)
        logging.info("wait for %d packets, current states: %s", waitPckCount, [pck.state for pck in pckList])
        if waitPckCount == 0:
            return [pck.state for pck in pckList]
        time.sleep(timeout)


def PrintCurrentWorkingJobs(queue):
    workingPackets = queue.ListPackets("working")
    if not workingPackets:
        logging.info("empty working set")
    for pck in workingPackets:
        for job in pck.jobs:
            if job.state == "working":
                logging.info("working packet %s: \"%s\"", pck.name, job.shell)


class ServiceTemporaryShutdown(object):
    def __init__(self, path_to_daemon='./'):
        self.cmd = os.path.join(path_to_daemon, 'start-stop-daemon.py')

    def __enter__(self):
        try:
            subprocess.check_call([self.cmd, "stop"])
        except OSError:
            logging.exception("can't stop service")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        subprocess.check_call([self.cmd, "start"])


def RestartService(path_to_daemon="./"):
    cmd = os.path.join(path_to_daemon, "start-stop-daemon.py")
    subprocess.check_call([cmd, "restart"])


class RemServerWrapper(object):
    def __init__(self, conf):
        self.conf = conf

    @property
    def connector(self):
        return self.conf.connector

    @property
    def admin_connector(self):
        return self.conf.admin_connector

    @property
    def name(self):
        return self.conf.name

    def Restart(self):
        RestartService(self.conf.projectDir)

    def TemporaryShutdown(self):
        return ServiceTemporaryShutdown(self.conf.projectDir)

    def Tag(self, prefix, digits=0):
        return self.TagWrapper(prefix + ('-%.' + str(digits) + 'f') % time.time(), self)

    def tag_local_name(self, tag):
        return tag.local_name_for(self)

    def SuccessfullPacket(self, prefix=None, wait=None, set=None):
        if prefix is None:
            name = 'pck_%08x_%.6f' % (random.getrandbits(32), time.time())
        else:
            name = prefix + '-%.0f' % time.time()

        if wait and not isinstance(wait, list):
            wait = [wait]

        pck = self.connector.Packet(
            name,
            set_tag=self.tag_local_name(set) if set is not None else None,
            wait_tags=map(self.tag_local_name, wait) if wait is not None else [],
        )

        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)

        return pck

    class TagWrapper(object):
        def __init__(self, name, rem):
            self.rem  = rem
            self.name = name
            self._impl = self.rem.connector.Tag(self.name)

        def Set(self):
            self._impl.Set()

        def Unset(self):
            self._impl.Unset()

        def Reset(self, message=None):
            self._impl.Reset(message)

        def local_name_for(self, rem):
            return self.name if self.rem.name == rem.name \
                else ('%s:%s' % (self.rem.name, self.name))

        def AsNative(self, rem):
            return rem.connector.Tag(self.local_name_for(rem))
