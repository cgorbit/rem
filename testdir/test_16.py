import unittest
import logging
import remclient
import time
from testdir import *


class T16(unittest.TestCase):
    """Test for queue/level lifetimes functionality"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.connector2 = Config.Get().server2.connector

    def testCustomQueueSuccessLifetime(self):
        timestamp = time.time()
        pck = self.connector.Packet('test_successfull_lifetime-%d' % int(timestamp))
        pck.AddJob('true')
        self.connector.Queue('test_lifetime').AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        WaitForExecution(pckInfo, ["SUCCESSFULL"])

        self.connector.Queue('test_lifetime').SetSuccessLifeTime(1)
        time.sleep(1)
        self.connector.proxy.forget_old_items()

        self.assertRaises(pckInfo.update)

    def testCustomQueueSuccessLifetime(self):
        timestamp = time.time()
        pck = self.connector.Packet('test_error_lifetime-%d' % int(timestamp))
        pck.AddJob('false', tries=1)
        self.connector.Queue('test_lifetime').AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        WaitForExecution(pckInfo, ["ERROR"])

        self.connector.Queue('test_lifetime').SetErroredLifeTime(1)
        time.sleep(1)
        self.connector.proxy.forget_old_items()

        self.assertRaises(pckInfo.update)

    def testCustomQueueSuccessLifetime(self):
        timestamp = time.time()
        queue = self.connector.Queue('test_lifetime_%s' % timestamp)

        pck = self.connector.Packet('test_suspended_lifetime-%d' % int(timestamp),
            wait_tags=['no_such_tag_%s' % timestamp])
        pck.AddJob('true')
        queue.AddPacket(pck)

        pckInfo = self.connector.PacketInfo(pck.id)

        WaitForExecution(pckInfo, ["SUSPENDED"])

        queue.SetSuspendedLifeTime(1)
        time.sleep(1)
        self.connector.proxy.forget_old_items()

        WaitForExecution(pckInfo, ["ERROR"])
