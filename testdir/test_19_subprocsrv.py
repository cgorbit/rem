import sys
import time
import os
#import logging

import unittest
from tempfile import NamedTemporaryFile

import rem.subprocsrv as subprocsrv
import rem.process_proxy
#import rem.pgrpguard

# TODO base_test(pgrpguard_binary=None){None, which}
# TODO Pool
# TODO FallbackRunner

# TODO self.setpgrp = setpgrp # useless with use_pgrpguard=True
# TODO self.cwd = cwd # filename or None
# TODO self.shell = shell # filename or None
# TODO stdin_content

class T19(unittest.TestCase):
    def testXXX(self):
        self._testXXX(subprocsrv.Runner(pgrpguard_binary=None), use_pgrpguard=False)

    def testYYY(self):
        self._testXXX(subprocsrv.Runner(pgrpguard_binary='pgrpguard'), use_pgrpguard=True)

    def testZZZ(self):
        self._testXXX(subprocsrv.FallbackkedRunner(subprocsrv.BrokenRunner()))

    def _testXXX(self, runner, use_pgrpguard=False):
        msg = str(time.time())

        with NamedTemporaryFile('w') as stdin:
            with NamedTemporaryFile('r') as stderr:
                with NamedTemporaryFile('r') as stdout:
                    stdin.write(msg)
                    stdin.flush()

                    p = runner.Popen(
                        'cat; echo -n Error >&2; exit 3',
                        shell=True,
                        setpgrp=False,
                        stdin=stdin.name,
                        stdout=stdout.name,
                        stderr=stderr.name,
                        use_pgrpguard=use_pgrpguard
                    )

                    self.assertEqual(p.wait(), 3)

                    self.assertEqual(stdout.read(), msg)
                    self.assertEqual(stderr.read(), 'Error')

