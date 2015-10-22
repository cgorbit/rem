import logging
import time
import unittest
import subprocess

import remclient
from testdir import *

def all_eq(value, iterable):
    return all(item == value for item in iterable)

class T10(unittest.TestCase):
    """Check remote tags"""

    def setUp(self):
        self.remA = RemServerWrapper(Config.Get().server1)
        self.remB = RemServerWrapper(Config.Get().server2)

    def _wait_successful(self, pck):
        if isinstance(pck, list):
            self.assertTrue(all_eq('SUCCESSFULL', WaitForStates(pck)))
        else:
            self.assertEqual(WaitForStates(pck), 'SUCCESSFULL')

    def _wait_suspended(self, pck):
        fin_states = ['SUSPENDED', 'ERROR']

        if isinstance(pck, list):
            self.assertTrue(all_eq('SUSPENDED', WaitForStates(pck, fin_states)))
        else:
            self.assertEqual(WaitForStates(pck, fin_states), 'SUSPENDED')

    def _check_tag_set(self, tag, rem):
        self.assertTrue(tag.AsNative(rem).Check())

    def _check_tag_not_set(self, tag, rem):
        self.assertTrue(not tag.AsNative(rem).Check())

    def _sync_tags(self, src, dst):
        self.assertTrue(src.name != dst.name)

        marker = src.Tag('send_marker', digits=6)
        self._check_tag_not_set(marker, dst)
        marker.Set()

        pck = dst.SuccessfullPacket(wait=marker)
        self._wait_successful(pck)

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

        class Side(object):
            def __init__(self):
                self.packets = []
                self.tags = []

        starter = remB.Tag("mass_remote_ancestor")
        sideA = Side()
        sideB = Side()

        for i in xrange(400):
            tagA = remA.Tag("mass_remote_tag_1_%d" % i)
            tagB = remB.Tag("mass_remote_tag_2_%d" % i)

            sideB.tags.append(tagB)

            sideA.packets.append(
                remA.SuccessfullPacket("pck1_%d" % i, set=tagA, wait=starter))

            sideB.packets.append(
                remB.SuccessfullPacket("pck2_%d" % i, set=tagB, wait=[tagA, starter]))

        logging.debug(remA.admin_connector.ListDeferedTags(remA.name))
        logging.debug(remA.admin_connector.ListDeferedTags(remB.name))

        end_pck = remA.SuccessfullPacket("pck_many", wait=sideB.tags)

        def wait_successfull():
            for p in [sideA.packets, sideB.packets, end_pck]:
                self._wait_successful(p)

        def wait_suspended():
            for p in [sideA.packets, sideB.packets, end_pck]:
                self._wait_suspended(p)

        def sync():
            self._sync_tags(remA, remB)

        wait_suspended()

        starter.Set()
        sync()
        wait_successfull()

        starter.Unset()
        sync()
        time.sleep(3)
        wait_successfull()

        starter.Reset('reason')
        sync()
        wait_suspended()

        starter.Unset()
        sync()
        time.sleep(3)
        wait_suspended()

        starter.Set()
        sync()
        wait_successfull()

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
        remA, remB = self.get_rems()

        tagA = remA.Tag('events')

        def sync():
            self._sync_tags(remA, remB)

        def test_set():
            tagA.Set()
            sync()
            self._wait_successful(pck)
            self._check_tag_set(tagA, remB)

        def unset():
            tagA.Unset()
            sync()

        def test_reset():
            tagA.Reset('some reason')
            sync()
            self._wait_suspended(pck)
            self._check_tag_not_set(tagA, remB)

        pck = remB.SuccessfullPacket('pck_events', wait=tagA)

        self._wait_suspended(pck)

        test_set()

        unset()
        self._check_tag_not_set(tagA, remB)
        self._wait_successful(pck)

        test_reset()

        unset()
        self._check_tag_not_set(tagA, remB)
        self._wait_suspended(pck)

        test_set()

        test_reset()

    def testCheckConnection(self):
        self.assertTrue(self.remA.admin_connector.CheckConnection(self.remB.name))
