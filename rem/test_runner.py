from time import sleep
import logging
import os
import sys
import time
import threading
import random

sys.path[0:0] = ['/home/trofimenkov/rem']

import subprocsrv

def readlines():
    while True:
        line = sys.stdin.readline()
        if not line:
            return
        yield line

def sleeps_thread(use_pgrpguard):
    while True:
        argv = ['sleep', '%.1f' % (random.random() * 5)]

        #subprocsrv.call(argv)
        subprocsrv.Popen(argv, use_pgrpguard=use_pgrpguard).wait()

def theloop(use_pgrpguard=False):
    for line in readlines():
        command = line.rstrip('\n')
        if not command:
            continue

        logging.debug("BEFORE RUN")
        try:
            proc = subprocsrv.Popen(
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

    #subprocsrv.LOG_LEVEL = subprocsrv.LL_DEBUG

    pgrpguard_binary = sys.argv[1] if len(sys.argv) > 1 else None

    subprocsrv.ResetDefaultRunner(pgrpguard_binary=pgrpguard_binary, pool_size=10)

    #subprocsrv.ResetDefaultRunner(
        #runner=subprocsrv.FallbackkedRunner(
            ##subprocsrv.RunnerPool(3, pgrpguard_binary)
            #subprocsrv.BrokenRunner()
        #)
    #)

    print >>sys.stderr, '__main__ pid:', os.getpid()

    use_pgrpguard = bool(pgrpguard_binary)

    if  False:
        try:
            theloop(use_pgrpguard)
        except KeyboardInterrupt:
            subprocsrv.DEFAULT_RUNNER.stop()

    elif False:
        threads = [
            threading.Thread(target=sleeps_thread, args=(use_pgrpguard,))
                for _ in xrange(300)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    elif True:
        count = 0
        prev_t = time.time()
        while True:
            if False:
                joiners = []
                for _ in xrange(50):
                    join_pid = subprocsrv.start(['true'])
                    joiners.append(join_pid)
                    count += 1

                for join in joiners:
                    join()

            elif True:
                join = subprocsrv.start(['sleep', '1'])
                join()
                count += 1

            else:
                raise RuntimeError()

            if count == 1000:
                t = time.time()
                print >>sys.stderr, 1 / ((t - prev_t) / count)
                prev_t = t
                count = 0

    else:
        pass

    #subprocsrv.DestroyDefaultRunnerIfNeed()
