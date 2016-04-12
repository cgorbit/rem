import os
import sys
import time

import unittest
from tempfile import NamedTemporaryFile

import rem.subprocsrv as subprocsrv
import rem.job

class T21(unittest.TestCase):
    def test000(self):
        self._test_common(subprocsrv.Runner(pgrpguard_binary=None), use_pgrpguard=False)

    def test001(self):
        self._test_common(subprocsrv.Runner(pgrpguard_binary='pgrpguard'), use_pgrpguard=False)

    def test002(self):
        self._test_common(subprocsrv.Runner(pgrpguard_binary='pgrpguard'), use_pgrpguard=True)

    def test003(self):
        self._test_common(subprocsrv.BrokenRunner(), use_pgrpguard=False)

    def test004(self):
        self._test_common(subprocsrv.BrokenRunner(), use_pgrpguard=True)

    def test005(self):
        self._test_common(None, use_pgrpguard=False)

    def test006(self):
        self._test_common(None, use_pgrpguard=True)

    def _test_common(self, runner, use_pgrpguard):
        for do_terminate in [False, True]:
            self._do_test_common(runner, use_pgrpguard, do_terminate)

    def _do_test_common(self, runner, use_pgrpguard, do_terminate):
        pgrpguard_binary = 'pgrpguard' if use_pgrpguard else None
        start_process = rem.job.create_job_runner(runner, pgrpguard_binary)

        msg = str(time.time())

        with NamedTemporaryFile('w') as parent_stdin:
            with NamedTemporaryFile('r') as parent_stderr:
                with NamedTemporaryFile('r') as parent_stdout:
                    parent_stdin.write(msg)
                    parent_stdin.flush()

                    with open(parent_stdin.name, 'r') as stdin:
                        with open(parent_stdout.name, 'w') as stdout:
                            with open(parent_stderr.name, 'w') as stderr:
                                p = start_process(
                                    ['sh', '-c', 'cat; pwd >&2; sleep 3; exit 3'],
                                    cwd='/proc',
                                    stdin=stdin,
                                    stdout=stdout,
                                    stderr=stderr,
                                )

                    time.sleep(1)
                    self.assertEqual(p.poll(), None)

                    if do_terminate:
                        p.terminate()

                    if do_terminate:
                        #self.assertTrue(p.wait() in [-15, -9])
                        self.assertEqual(p.wait(), -15)
                    else:
                        self.assertEqual(p.wait(), 3)

                    self.assertEqual(p.was_signal_sent(), do_terminate)

                    self.assertEqual(parent_stdout.read(), msg)
                    self.assertEqual(parent_stderr.read(), '/proc\n')

