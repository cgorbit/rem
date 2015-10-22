import logging
import time
import unittest
import subprocess

import remclient
from testdir import *

class T10(unittest.TestCase):
    """Check remote tags"""

    def setUp(self):
        self.remA = RemServerWrapper(Config.Get().server1)
        self.remB = RemServerWrapper(Config.Get().server2)

    def get_rems(self):
        return self.remA, self.remB

    def testNetworkUnavailableSubscribe(self):
        remA, remB = self.get_rems()

        tagA = remA.Tag("nonetwork-tag")

        tagA.Set()

        with remA.TemporaryShutdown():
            pck = remB.SuccessfullPacket(wait=tagA)
            time.sleep(4)

        self.assertEqual(WaitForStates(pck), "SUCCESSFULL")


    def testNetworkUnavailableSetTag(self):
        remA, remB = self.get_rems()

        tagA = remA.Tag("network-tag")

        pck = remB.SuccessfullPacket(wait=tagA)

        with remB.TemporaryShutdown():
            tagA.Set()
            time.sleep(4)

        self.assertEqual(WaitForStates(pck), "SUCCESSFULL")


    def testRemoteTag(self):
        remA, remB = self.get_rems()

        tagA = remA.Tag("remotetag1")
        tagB = remB.Tag("remotetag2")

        pck01 = remA.SuccessfullPacket(set=tagA)

        pck02 = remB.SuccessfullPacket(wait=tagA, set=tagB)

        pck03 = remA.SuccessfullPacket(wait=tagB)

        self.assertEqual(WaitForStates(pck03), "SUCCESSFULL")


    def testManyRemoteTags(self):
        remA, remB = self.get_rems()

        remote_tags = []
        for i in xrange(400):
            tagA = remA.Tag("mass_remote_tag_1_%d" % i)
            tagB = remB.Tag("mass_remote_tag_2_%d" % i)

            remote_tags.append(tagB)

            remA.SuccessfullPacket("pck1_%d" % i, set=tagA)
            remB.SuccessfullPacket("pck2_%d" % i, set=tagB, wait=tagA)

        logging.debug(remA.admin_connector.ListDeferedTags(remA.name))
        logging.debug(remA.admin_connector.ListDeferedTags(remB.name))

        pck = remA.SuccessfullPacket("pck_many", wait=remote_tags)

        self.assertEqual(WaitForStates(pck), "SUCCESSFULL")


    def testSubscribing(self):
        remA, remB = self.get_rems()

        tag01 = remA.Tag("subscription-tag-1")
        tag02 = remA.Tag("subscription-tag-2")

        remA.Restart()

        remA.SuccessfullPacket(set=tag01)
        remA.SuccessfullPacket(set=tag02)

        pck1 = remB.SuccessfullPacket("pck1", wait=tag01)
        pck2 = remB.SuccessfullPacket("pck2", wait=tag02)

        time.sleep(2)

        remA.Restart()

        WaitForStates([pck1, pck2])


    def testRestoringTagFromFile(self):
        remA, remB = self.get_rems()

        tagA = remA.Tag("restoring-tag")

        tagA.Set()
        tagA.Unset()

        remA.Restart()

        pck = remB.SuccessfullPacket(wait=tagA)

        time.sleep(3)
        tagA.Set()

        self.assertEqual(WaitForStates(pck), "SUCCESSFULL")


    def testAllEventType(self):
        def wait_successful(pck):
            self.assertEqual(WaitForStates(pck), 'SUCCESSFULL')

        def wait_suspended(pck):
            self.assertEqual(
                WaitForStates(pck, ['SUSPENDED', 'SUCCESSFULL', 'ERROR']),
                'SUSPENDED')

        def check_tag_set(tag, rem):
            self.assertTrue(tag.AsNative(rem).Check())

        def check_tag_not_set(tag, rem):
            self.assertTrue(not tag.AsNative(rem).Check())

        def sync_tags(src, dst):
            self.assertTrue(src.name != dst.name)

            marker = src.Tag('send_marker', digits=6)
            check_tag_not_set(marker, dst)
            marker.Set()

            pck = dst.SuccessfullPacket(wait=marker)
            wait_successful(pck)

        remA, remB = self.get_rems()

        tagA = remA.Tag('events')

        def sync():
            sync_tags(remA, remB)

        def test_set():
            tagA.Set()
            sync()
            wait_successful(pck)
            check_tag_set(tagA, remB)

        def unset():
            tagA.Unset()
            sync()

        def test_reset():
            tagA.Reset('some reason')
            sync()
            wait_suspended(pck)
            check_tag_not_set(tagA, remB)

        pck = remB.SuccessfullPacket('pck_events', wait=tagA)

        wait_suspended(pck)

        test_set()

        unset()
        check_tag_not_set(tagA, remB)
        wait_successful(pck)

        test_reset()

        unset()
        check_tag_not_set(tagA, remB)
        wait_suspended(pck)

        test_set()

        test_reset()

    def testCheckConnection(self):
        self.assertTrue(self.remA.admin_connector.CheckConnection(self.remB.name))
