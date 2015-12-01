import logging
import hashlib
import time
import unittest
import os
import tempfile

import remclient
from testdir import *


class T08(unittest.TestCase):
    """Check restarting tag"""

    def setUp(self):
        self.connector = Config.Get().server1.connector
        self.srvdir = Config.Get().server1.projectDir
        self.remA = RemServerWrapper(Config.Get().server1)
        self.remB = RemServerWrapper(Config.Get().server2)

    def testOneTry(self):
        """Test for jobs with retries=1"""
        tag = "tag-one-try-%.0f" % time.time()
        pck = self.connector.Packet("pck-one-try", wait_tags=[tag])
        j1 = pck.AddJob("sleep 1", tries=1)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.connector.Tag(tag).Set()
        time.sleep(0.1)
        self.connector.Tag(tag).Reset()
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo, timeout=1.0), "SUCCESSFULL")
        pckInfo.Delete()

    def testSuccessfullPacket(self):
        tag = "tag-successfull-%.0f" % time.time()
        pck1 = self.connector.Packet("pck-successfull-1", set_tag=tag)
        j = pck1.AddJob("sleep 3")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck1)
        pckInfo1 = self.connector.PacketInfo(pck1.id)

        pck2 = self.connector.Packet("pck-successfull-2", wait_tags=[tag])
        j = pck2.AddJob("sleep 3")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck2)
        pckInfo2 = self.connector.PacketInfo(pck2.id)

        self.assertEqual(WaitForExecution(pckInfo1, timeout=1.0), "SUCCESSFULL")

        self.connector.Tag(tag).Reset()
        time.sleep(2)
        self.assertNotEqual(pckInfo2.state, "SUCCESSFULL")
        self.connector.Tag(tag).Set()
        time.sleep(1)
        WaitForExecutionList([pckInfo1, pckInfo2], timeout=1.0)
        time.sleep(1)
        logging.info("%s %s", pckInfo1.pck_id, pckInfo2.pck_id)
        time.sleep(1)
        # for pck in [pckInfo1, pckInfo2]:
        # # self.assertEqual(pck.state, "SUCCESSFULL")
        # pck.Delete()

    def testRestartPacketAddFiles(self):
        rem = self.remA

        def do_test(suspend, add_files, reset_tag):
            if add_files and reset_tag:
                return

            parent_tag = rem.Tag("pck_reset_parent", digits=6)
            child_tag = rem.Tag("pck_reset_child", digits=6)
            if reset_tag:
                parent_tag.Set()
                pck_setup = rem.PacketSetup("pck-restart-indirect", wait=parent_tag, set=child_tag, digits=6)
                with pck_setup as child_pck:
                    child_pck.AddJob("false", tries=1)

                self.assertEqual(WaitForStates(child_pck, timeout=1.0), "ERROR")
            child_tag.Set()
            parent_tag.Unset()

            with rem.PacketSetup("pck-restart", set=parent_tag, digits=6) as pck:
                sleep = pck.AddJob("sleep 3", tries=1)
                pck.AddJob("./true" if add_files else '/bin/true', tries=1, parents=[sleep])

            pck = rem.PacketInfo(pck)

            self.assertEqual(WaitForStates(pck, timeout=1.0), "ERROR" if add_files else "SUCCESSFULL")

            pck.Restart(
                files=['/bin/true'] if add_files else None,
                suspend=suspend,
                reset_tag=reset_tag,
                reset_message="Reset message" if reset_tag else None,
            )

            time.sleep(2)

            state_to_wait = 'SUSPENDED' if suspend else 'WORKABLE'
            self.assertEqual(
                WaitForStates(
                    pck,
                    ['SUCCESSFULL', 'ERROR', state_to_wait],
                    timeout=1.0
                ),
                state_to_wait
            )

            if suspend:
                pck.Resume()

            self.assertEqual(WaitForStates(pck, timeout=1.0), "SUCCESSFULL")

            self.assertTrue(bool(reset_tag) != bool(child_tag.IsSet()))

        bools = [False, True]
        for suspend in bools:
            for add_files in bools:
                for reset_tag in bools:
                    do_test(suspend, add_files, reset_tag)

    def testRestoringDirectory(self):
        tag = "tag-start-%.0f" % time.time()
        pck = self.connector.Packet("pck-restore-dir", wait_tags=[tag])
        j = pck.AddJob("sleep 1")
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        with ServiceTemporaryShutdown(self.srvdir):
            path = os.path.join("packets", pck.id)
            self.assertFalse(os.path.isdir(path))
        self.connector.Tag(tag).Reset()
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        pckInfo.Delete()

    def testRestoringExistingFile(self):
        """Restoring file from bin directory"""
        tag = "tag-start-%.0f" % time.time()
        pck = self.connector.Packet("pck-restore-file", wait_tags=[tag])
        with tempfile.NamedTemporaryFile(dir=".") as script_printer:
            print >> script_printer, "#!/usr/bin/env python"
            print >> script_printer, "print >>open('../test', 'w'), 42"
            script_printer.flush()
            j = pck.AddJob("sleep 1 && ./testfile.py", files={"testfile.py": script_printer.name}, tries=2)
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        with tempfile.NamedTemporaryFile(dir=".") as script_printer:
            print >> script_printer, "#!/usr/bin/env python"
            print >> script_printer, "print >>open('../test', 'w'), 43"
            script_printer.flush()
            pck.AddFiles({"testfile.py": script_printer.name})
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        with ServiceTemporaryShutdown(self.srvdir):
            path = os.path.join("packets", pck.id)
            self.assertFalse(os.path.isdir(path))
        self.connector.Tag(tag).Reset()
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        pckInfo.Delete()

    def testRestoringNonexistingFile(self):
        """Restoring file from bin directory which has been removed
        packet should change state to WAITING"""
        tag = "tag-start-%.0f" % time.time()
        pck = self.connector.Packet("pck-restore-file", wait_tags=[tag])
        with tempfile.NamedTemporaryFile(dir=".") as script_printer:
            print >> script_printer, "#!/usr/bin/env python"
            print >> script_printer, "print 42"
            script_printer.flush()
            j = pck.AddJob("sleep 1 && ./testfile.py", files={"testfile.py": script_printer.name}, tries=2)
            with open(script_printer.name, "r") as reader:
                md5sum = hashlib.md5(reader.read()).hexdigest()
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        pckInfo = self.connector.PacketInfo(pck.id)
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo), "SUCCESSFULL")
        with ServiceTemporaryShutdown(self.srvdir):
            path = os.path.join(Config.Get().server1.binDir, md5sum)
            self.assertTrue(os.path.isfile(path))
            os.remove(path)
        self.connector.Tag(tag).Reset()
        self.connector.Tag(tag).Set()
        self.assertEqual(WaitForExecution(pckInfo, ("WAITING", "SUCCESSFULL", "ERROR")), "WAITING")
        pckInfo.Suspend()
        pckInfo.Delete()

    def testDoneTags(self):
        """Test reset packet with done tags in wait_tags"""
        tag = "tag-done-tags-%.0f" % time.time()
        tag1, tag2 = tag + "1", tag + "2"
        pck = self.connector.Packet("pck-done-tags-%.0f" % time.time(), wait_tags=[tag1, tag2])
        self.connector.Queue(TestingQueue.Get()).AddPacket(pck)
        self.connector.Tag(tag1).Set()
        pckInfo = self.connector.PacketInfo(pck.id)
        pckInfo.Restart()
        self.connector.Tag(tag2).Set()
        self.assertEqual(WaitForExecution(pckInfo, timeout=1.0), "SUCCESSFULL")
        pckInfo.Delete()
