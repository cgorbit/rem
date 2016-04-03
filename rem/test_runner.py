from time import sleep
import logging
import os
import sys
import threading
import random

sys.path[0:0] = ['/home/trofimenkov/rem']

import runproc
runproc.LOG_LEVEL = runproc.LL_DEBUG

def readlines():
    while True:
        line = sys.stdin.readline()
        if not line:
            return
        yield line

def sleeps_thread(use_pgrpguard):
    while True:
        argv = ['sleep', '%.1f' % (random.random() * 5)]

        #runproc.call(argv)
        runproc.Popen(argv, use_pgrpguard=use_pgrpguard).wait()

def theloop(use_pgrpguard=False):
    for line in readlines():
        command = line.rstrip('\n')
        if not command:
            continue

        logging.debug("BEFORE RUN")
        try:
            proc = runproc.Popen(
                filter(None, line.rstrip('\n').split(' ')),
                use_pgrpguard=use_pgrpguard
            )
            logging.debug("AFTER Popen returns")
        except Exception as e:
            print 'except:', repr(e)
        else:
            try:
                proc.wait()
            except Exception as e:
                logging.exception("proc.returncode = raise")
                #print >>sys.stderr, 'except:', repr(e)
            else:
                logging.debug("proc.returncode = %s" % proc.returncode)
        logging.debug("AFTER RUN")


if __name__ == '__main__':
    #logging.basicConfig(format="%(asctime)s %(levelname)-8s %(module)s:\t%(message)s")
    logging.basicConfig(format="%(asctime)s %(message)s")
    logging.root.setLevel(logging.DEBUG)

    pgrpguard_binary = sys.argv[1] if len(sys.argv) > 1 else None

    #runproc.ResetDefaultRunner(pgrpguard_binary=pgrpguard_binary)
    runproc.ResetDefaultRunner(
        runner=runproc.FallbackkedRunner(
            #runproc.RunnerPool(3, pgrpguard_binary)
            runproc.BrokenRunner()
        )
    )

    print >>sys.stderr, '__main__ pid:', os.getpid()

    use_pgrpguard = bool(pgrpguard_binary)

    if True:
        try:
            theloop(use_pgrpguard)
        except KeyboardInterrupt:
            runproc.DEFAULT_RUNNER.stop()

    elif True:
        threads = [threading.Thread(target=sleeps_thread, args=(use_pgrpguard,)) for _ in xrange(300)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    else:
        pass

    #runproc.DestroyDefaultRunnerIfNeed()
