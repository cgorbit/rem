import os
import sys
import signal
import subprocess
import errno
import time
import socket
import select
import logging

import unittest

import rem.pgrpguard as pgrpguard
from rem.pgrpguard import _EXIT_STATUS_IN_FILE, ProcessGroupGuard


def sudo_get_open_file_count(pid):
    p = subprocess.Popen(
        ['sudo', 'sh', '-c', 'ls -1 /proc/%d/fd | wc -l' % pid],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    out, err = p.communicate()

    if p.returncode:
        raise RuntimeError("Failed to get open file count for pid=%d" % pid)

    return int(out)


def get_process_uids(pid):
    with open('/proc/%d/status' % pid) as in_:
        for line in in_:
            if line.startswith('Uid:'):
                return map(int, line.rstrip('\n').split('\t')[1:])


def readlines(stream):
    while True:
        line = stream.readline()
        if not line:
            return
        yield line.rstrip('\n')


def GuardedProcessCWDWrapper(*args, **kwargs):
    return ProcessGroupGuard(*args, wrapper_binary='pgrpguard', **kwargs)


class SpareFileDescriptors(object):
    def __enter__(self):
        self._pipes = [os.pipe() for _ in range(10)]

    def __exit__(self, e, t, tb):
        for rd, wr in self._pipes:
            os.close(rd)
            os.close(wr)


def with_spare_file_desriptors(f):
    def impl(*args):
        with SpareFileDescriptors():
            return f(*args)

    impl.__name__ = f.__name__
    return impl

class T20(unittest.TestCase):
    @with_spare_file_desriptors
    def testWrapperNotFound(self):
        ok = False
        try:
            ProcessGroupGuard(
                ['false'],
                wrapper_binary='/hello',
                close_fds=True
            )
        except pgrpguard.WrapperNotFoundError as e:
            ok = True

        if not ok:
            raise RuntimeError("")

    @with_spare_file_desriptors
    def testOpenFileCount(self):
        p = GuardedProcessCWDWrapper(
            ['python'],
            close_fds=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertEqual(sudo_get_open_file_count(p.pid), 1) # only report_fd in wrapper

        out, err = p.communicate('''
import sys
import os
import errno

pid = os.fork()
if pid:
    os.waitpid(pid, 0)
else:
    os.chdir('/proc/%d/fd' % os.getppid())

    #for fd in os.listdir('.'):
        #path = os.readlink(fd)
        #print '%s -> %s' % (fd, path)
        #print fd

    sys.stdout.write(','.join(os.listdir('.')))
    sys.stderr.write('test_open_file_count')
''')

        self.assertEqual(p.wait(), 0)
        self.assertEqual(out, '0,1,2')
        self.assertEqual(err, 'test_open_file_count')

    @with_spare_file_desriptors
    def testExitStatuses(self):
        start_time = str(time.time())

        env = os.environ.copy()
        env.update([('START_TIME', start_time)])

        def run(cmd):
            p = GuardedProcessCWDWrapper(
                cmd,
                close_fds=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
            out, err = p.communicate('INPUT')
            return p.returncode, out, err

        self.assertEqual(
            run(['sh', '-c', 'exit 0']),
            (0, '', '')
        )

        self.assertEqual(
            run(['sh', '-c', 'echo -n Out; echo -n Err >&2; exit 5']),
            (5, 'Out', 'Err')
        )

        self.assertEqual(
            run(['sh', '-c', 'echo -n $START_TIME']),
            (0, start_time, '')
        )

        self.assertEqual(
            run(['sh', '-c', 'echo -n Out; echo -n Err >&2; exit %d' % _EXIT_STATUS_IN_FILE]),
            (_EXIT_STATUS_IN_FILE, 'Out', 'Err')
        )

        self.assertEqual(
            run(['sh', '-c', 'echo -n Out; echo -n Err >&2; kill -SEGV $$']),
            (-signal.SIGSEGV, 'Out', 'Err')
        )

    @with_spare_file_desriptors
    def testKill1(self):
        self._testKill(True)

    @with_spare_file_desriptors
    def testKill2(self):
        self._testKill(False)

    def _testKill(self, ignore_term_in_leader):
        p = GuardedProcessCWDWrapper(
            ['python'],
            close_fds=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        def create_listen_sock():
            ret = socket.socket(socket.AF_INET6, socket.SOCK_STREAM, 6)
            ret.bind(('::', 0))
            ret.listen(10)
            return ret

        listen_sock = create_listen_sock()
        child_count = 3

        p.stdin.write('''
import time
import sys
import os
import socket
import signal

timeout = 120
child_count = int({child_count})
port = int({port})

print os.getpid()

def run_connect():
    sock = socket.create_connection(('::', port))._sock
    sock.send(str(os.getpid()))
    time.sleep(timeout)

def run_sudo():
    os.execvp(
        'sudo',
        ['sudo', '-u', 'alex-sh',
            'bash', '-c',
                '{{ echo $$; sleep 120; }} | nc -w 30 :: %d; sleep 120' % port
        ]
    )

for _ in range(child_count):
    pid = os.fork()
    if pid:
        print pid
    else:
        os.close(1)
        try:
            if _:
                run_connect()
            else:
                run_sudo()
        except Exception as e:
            print >>sys.stderr, e
            sys.stderr.flush()
            os._exit(1)
        os._exit(0)

sys.stdout.flush()
sys.stdout.close()
os.close(1)

if int({ignore_term_in_leader}):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

run_connect()

'''.format(
    child_count=child_count,
    port=listen_sock.getsockname()[1],
    ignore_term_in_leader=int(ignore_term_in_leader)
))

        p.stdin.close()

        pids = map(int, readlines(p.stdout))

        uids = [get_process_uids(pid)[0] for pid in pids]

        #print >>sys.stderr, 'run.py:', os.getpid()
        #print >>sys.stderr, 'pgrpguard:', p.pid
        #print >>sys.stderr, 'leader + group:', pids
        #print >>sys.stderr, 'nearest uids:', uids

        clients = []
        clients_pids = []
        while len(clients) < child_count + 1:
            rout = select.select([listen_sock.fileno()], [], [], 3)[0]
            if not rout:
                raise RuntimeError("Timeout on client connect")

            clients.append(listen_sock.accept()[0])

            if not select.select([clients[-1].fileno()], [], [], 1.0)[0]:
                raise RuntimeError("Timeout on client pid")

            clients_pids.append(int(clients[-1].recv(4096)))

            #print >>sys.stderr, '+ accept', clients_pids[-1]

        clients_uids = [get_process_uids(pid)[0] for pid in clients_pids]
        #print >>sys.stderr, 'clients uids:', clients_uids

        self.assertEqual(
            len({uid: None for uid in clients_uids}.keys()),
            2
        )

        fds = set(s.fileno() for s in clients)
        initial_fds = fds.copy()

        def get_ready(fds):
            deadline = time.time() + 1.0

            ready = []
            while True:
                timeout = deadline - time.time()
                if timeout < 0.0:
                    break

                rout = select.select(list(fds), [], [], timeout)[0]
                if not rout:
                    break

                fds -= set(rout)
                ready.extend(rout)

            return ready

        self.assertEqual(
            get_ready(fds),
            []
        )

        #print >>sys.stderr, subprocess.check_output(["pstree", "-aAplu", str(p.pid)])

        p.send_term_to_process()

        if ignore_term_in_leader:
            self.assertEqual(
                get_ready(fds),
                []
            )

            p.send_kill_to_group()

        self.assertEqual(
            set(get_ready(fds)),
            initial_fds
        )

        deadline = time.time() + 1.0
        while time.time() < deadline:
            if p.poll() is not None:
                break
            time.sleep(0.100)

        self.assertNotEqual(p.poll(), None)

        self.assertEqual(p.returncode, -9 if ignore_term_in_leader else -15)

        def read_stderr(stderr):
            err = ''
            deadline = time.time() + 2.0
            stderr_done = False

            while True:
                timeout = deadline - time.time()
                if timeout < 0.0:
                    break
                if not select.select([stderr.fileno()], [], [], timeout):
                    break

                s = os.read(stderr.fileno(), 4096)
                if not s:
                    stderr_done = True
                    break
                err += s

            self.assertEqual(stderr_done, True)

            return err

        err = read_stderr(p.stderr)

        if err:
            print >>sys.stderr, err
