import sys
import time
import os
#import logging

import unittest
from tempfile import NamedTemporaryFile

import rem.subprocsrv as subprocsrv
import rem.subprocsrv_fallback as subprocsrv_fallback

# TODO self.setpgrp = setpgrp # useless with use_pgrpguard=True
# TODO self.cwd = cwd # filename or None
# TODO self.shell = shell # filename or None

class T19(unittest.TestCase):
    """Test rem.common.proc_runner use cases"""

    def test001(self):
        self._test_common(subprocsrv.Runner(pgrpguard_binary=None), use_pgrpguard=False)

    def test002(self):
        self._test_common(subprocsrv.Runner(pgrpguard_binary='pgrpguard'), use_pgrpguard=False)

    def test003(self):
        self._test_common(subprocsrv.Runner(pgrpguard_binary='pgrpguard'), use_pgrpguard=True)

    def test004(self):
        self._test_common(subprocsrv_fallback.Runner())

    def test005(self):
        runner = subprocsrv_fallback.RunnerWithFallback(
            main=subprocsrv.BrokenRunner(),
            fallback=subprocsrv_fallback.Runner()
        )
        self._test_common(runner)

    def _do_test_common(self, runner, use_pgrpguard=False, use_stdin_content=False):
        msg = str(time.time())

        with NamedTemporaryFile('w') as stdin:
            with NamedTemporaryFile('r') as stderr:
                with NamedTemporaryFile('r') as stdout:
                    stdin.write(msg)
                    stdin.flush()

                    p = runner.Popen(
                        'cat; pwd >&2; exit 3',
                        cwd='/proc',
                        shell=True,
                        setpgrp=False,
                        stdin=stdin.name if not use_stdin_content else None,
                        stdin_content=msg if use_stdin_content else None,
                        stdout=stdout.name,
                        stderr=stderr.name,
                        use_pgrpguard=use_pgrpguard
                    )

                    self.assertEqual(p.wait(), 3)

                    self.assertEqual(stdout.read(), msg)
                    self.assertEqual(stderr.read(), '/proc\n')

    def _test_common(self, runner, use_pgrpguard=False):
        for use_stdin_content in [False, True]:
            self._do_test_common(runner,
                                 use_pgrpguard=use_pgrpguard,
                                 use_stdin_content=use_stdin_content)
