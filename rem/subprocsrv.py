import os
import sys
import fcntl
import socket
import cPickle as pickle
import threading
import logging
import signal
import time
from collections import deque
from tempfile import NamedTemporaryFile
import subprocess
from subprocess import CalledProcessError, MAXFD
import types
import errno
import weakref
import atexit

from future import Promise
from profile import ProfiledThread, set_thread_name
import pgrpguard


class ServiceUnavailable(RuntimeError):
    pass


class ProcessStartMessage(object):
    def __init__(self, task_id, pid, error):
        self.task_id = task_id
        self.pid = pid
        self.error = error


class ProcessTerminationMessage(object):
    def __init__(self, task_id, exit_status):
        self.task_id = task_id
        self.exit_status = exit_status


class StopServiceRequestMessage(object):
    pass


class StopServiceResponseMessage(object):
    pass


class NewTaskParamsMessage(object):
    def __init__(self, args, stdin=None, stdout=None, stderr=None, setpgrp=False,
                 cwd=None, shell=False, use_pgrpguard=False):
        self.task_id = None
        self.args = args
        self.stdin = stdin # string or None
        self.stdout = stdout # filename or None
        self.stderr = stderr # filename or None
        self.setpgrp = setpgrp # useless with use_pgrpguard=True
        self.cwd = cwd # filename or None
        self.shell = shell # filename or None
        self.use_pgrpguard = use_pgrpguard


class SendSignalRequestMessage(object):
    def __init__(self, process_task_id, sig, group):
        self.task_id = None
        self.process_task_id = process_task_id
        self.sig = sig
        self.group = group


class SendSignalResponseMessage(object):
    def __init__(self, task_id, was_sent, error):
        self.task_id = task_id
        self.was_sent = was_sent
        self.error = error


def _send_signal(pid, sig, group):
    kill = os.killpg if group else os.kill
    print >>sys.stderr, "+ %s(%s, %s)" % (kill, pid, sig)
    kill(pid, sig)


def _set_cloexec(fd):
    if not isinstance(fd, int):
        fd = fd.fileno()
    fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC | fcntl.fcntl(fd, fcntl.F_GETFD))


def set_nonblock(fd):
    if not isinstance(fd, int):
        fd = fd.fileno()
    fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NONBLOCK | fcntl.fcntl(fd, fcntl.F_GETFL))


#class PipeEvent(object):
    #def __init__(self):
        #self._rd, self._wr = os.pipe()
        #set_nonblock(self._wr)

    #@property
    #def fd(self):
        #return self._rd

    #def notify(self):
        #while True:
            #try:
                #os.write(self._wr, '\000')
                #break
            #except OSError as e:
                #if e.errno == errno.EINTR:
                    #continue
                #elif e.errno == errno.EAGAIN:
                    #break
                #else:
                    #raise

    #def wait(self):
        #while True:
            #try:
                #data = os.read(self._rd, 4096)
                #assert len(data)
                #break
            #except OSError as e:
                #if e.errno == errno.EINTR:
                    #continue
                #else:
                    #raise


def serialize(stream, data):
    return pickle.dump(data, stream, pickle.HIGHEST_PROTOCOL)


def deserialize(stream):
    return pickle.load(stream)


def dupfdopen(f, mode):
    return os.fdopen(os.dup(f.fileno()), mode)


def exit_on_error(func):
    def impl(self):
        try:
            func(self)
        except BaseException as e:
            try:
                os.write(2, str(e))
            except:
                pass
            os._exit(2)

    impl.__name__ = func.__name__

    return impl


LL_DEBUG = 90
LL_INFO  = 80
LL_ERROR = 20
LOG_LEVEL = LL_INFO


def _log(lvl, args):
    if LOG_LEVEL >= lvl:
        sys.stderr.write((args[0] % args[1:]) + '\n')


def logging_debug(*args):
    _log(LL_DEBUG, args)
def logging_info(*args):
    _log(LL_INFO, args)
def logging_error(*args):
    _log(LL_ERROR, args)


# XXX Don't use logging in _Server

class _Active(object):
    def __init__(self):
        self._by_pid = {}
        self._by_task_id = {}

    def add(self, task):
        self._by_pid[task.pid] = task
        self._by_task_id[task.task_id] = task

    def get_by_pid(self, pid):
        return self._by_pid[pid]

    def get_by_task_id(self, id):
        return self._by_task_id[id]

    def try_get_by_task_id(self, id):
        return self._by_task_id.get(id)

    def pop_by_pid(self, pid):
        task = self._by_pid.pop(pid)
        self._by_task_id.pop(task.task_id)

    def __nonzero__(self):
        return bool(self._by_pid)


class _Server(object):
    def __init__(self, channel, pgrpguard_binary=None):
        self._sig_chld_handler_pid = os.getpid() # #9535

        self._modify_signals(True)

        self.pgrpguard_binary = pgrpguard_binary

        self._channel = channel
        _set_cloexec(self._channel)

        self._channel_in = dupfdopen(channel, 'r')
        _set_cloexec(self._channel_in)

        self._channel_out = dupfdopen(channel, 'w')
        _set_cloexec(self._channel_out)

        self._should_stop = False
        self._lock = threading.Lock()
        self._active = _Active()
        self._running_count = 0

        self._send_queue = deque()
        self._send_queue_not_empty = threading.Condition(self._lock)

        self._read_stopped = False
        self._write_stopped = False

        self._read_thread = threading.Thread(target=self._read_loop)
        self._write_thread = threading.Thread(target=self._write_loop)
        self._read_thread.start()
        self._write_thread.start()

    def _modify_signals(self, set):
        signal.signal(signal.SIGCHLD, self._sig_chld_handler if set else signal.SIG_DFL)

        act = signal.SIG_IGN if set else signal.SIG_DFL
        for sig in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(sig, act)

        restart = bool(set)
        for sig in [signal.SIGCHLD, signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.siginterrupt(sig, not restart)

    def wait(self):
        # Fuck Python. Can't wait on mutexes here, because need
        # sleep, that never restarts on EINTR for _sig_chld_handler to run
        while not(self._read_stopped and self._write_stopped):
            self._read_thread.join(1) # sleeps for 50ms actually
            self._write_thread.join(1)
        self._read_thread.join()
        self._write_thread.join()

    class RunningTask(object):
        def __init__(self, task_id):
            self.task_id = task_id
            self.pid = None
            self.start_sent = False
            self.exec_errored = False
            self.exit_status = None
            self.error = None

        def on_before_fork(self):
            pass

        def on_after_fork(self):
            pass

        def on_fork_error(self):
            pass

        def on_before_exec(self):
            pass

        def on_exec_error(self, exc):
            return exc

        def on_process_termination(self, exit_status):
            self.exit_status = _handle_exitstatus(exit_status)

        def fix_args(self, args):
            return args

        def _check_pid_is_set(self):
            if not self.pid:
                raise RuntimeError(".pid is not set in %s" % type(self).__name__)

        def send_signal(self, sig, group):
            self._check_pid_is_set()
            _send_signal(self.pid, sig, group)

    class PgrpguardRunningTask(RunningTask):
        def __init__(self, task_id, pgrpguard_binary):
            _Server.RunningTask.__init__(self, task_id)
            self._pgrpguard_binary = pgrpguard_binary

        def on_before_fork(self):
            pipe = os.pipe()
            self._report_pipe = pipe
            _set_cloexec(pipe[0])
            _set_cloexec(pipe[1])

        def _close_fd(self, fd):
            try:
                os.close(fd)
            except OSError as e:
                if e.errno != errno.EINTR:
                    raise

        def on_after_fork(self):
            self._close_fd(self._report_pipe[1])

        def on_fork_error(self):
            for fd in self._report_pipe:
                self._close_fd(fd)

        def on_before_exec(self):
            pgrpguard._preexec_fn(self._report_pipe[1]) # FIXME Split _preexec_fn?

        def on_exec_error(self, exc):
            self._close_fd(self._report_pipe[0])
            return pgrpguard._interpret_exec_error(exc)

        def on_process_termination(self, status):
            if self.exec_errored:
                self.exit_status = _handle_exitstatus(status) # FIXME
                return

            self.exit_status = pgrpguard._real_status_from_report(
                self._report_pipe[0],
                _handle_exitstatus(status), # FIXME Better
                self._pgrpguard_binary)

            self._report_pipe = None

        def fix_args(self, args):
            return [self._pgrpguard_binary, str(self._report_pipe[1])] + args

        def send_signal(self, sig, group):
            self._check_pid_is_set()

            if sig == signal.SIGTERM and not group:
                pgrpguard.send_term_to_process(self.pid)

            elif sig == signal.SIGKILL and group:
                pgrpguard.send_kill_to_group(self.pid)

            else:
                raise RuntimeError("pgrpguard can't send signal %d (is_group=%d)" % (sig, group))

    def _start_process(self, task):
        if task.use_pgrpguard:
            if not self.pgrpguard_binary:
                return None, RuntimeError("No setup for pgrpguard")
            running_task = self.PgrpguardRunningTask(task.task_id, self.pgrpguard_binary)
        else:
            running_task = self.RunningTask(task.task_id)

        try:
            exec_err_rd, exec_err_wr = os.pipe()
        except OSError as e:
            return None, e

        try:
            running_task.on_before_fork()
        except Exception as e:
            return None, e

        with self._lock:
            before_fork_time = time.time()

            try:
                pid = os.fork()
            except OSError as e:
                try:
                    os.close(exec_err_rd)
                    os.close(exec_err_wr)
                except:
                    pass

                try:
                    running_task.on_fork_error()
                except Exception as e:
                    logging_error('on_fork_error failed: %s' % e)

                return None, e

            fork_time = time.time() - before_fork_time

            if pid:
                running_task.pid = pid
                self._active.add(running_task)
                self._running_count += 1

        if pid:
            logging_debug('fork time %s' % fork_time)

            try:
                running_task.on_after_fork()
            except Exception as e:
                logging_error('on_after_fork failed: %s' % e) # FIXME

            os.close(exec_err_wr)

            exec_error = None
            try:
                with os.fdopen(exec_err_rd) as in_:
                    s = in_.read()
                    if s:
                        exec_error = pickle.loads(s)
            except Exception as exec_error:
                try:
                    os.close(exec_err_rd)
                except:
                    pass
            else:
                if exec_error:
                    try:
                        exec_error = running_task.on_exec_error(exec_error)
                    except Exception as e:
                        logging_error('on_exec_error failed: %s' % e)

            running_task.exec_errored = bool(exec_error)
            return pid, exec_error or None

        else: # child
            exit_code = 0
            try:
                _set_cloexec(exec_err_wr)
                os.close(exec_err_rd)

                if task.stdin is not None:
                    dup2file(task.stdin, 0, os.O_RDONLY)
                else:
                    pass # /dev/null

                write_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

                if task.stdout is not None:
                    dup2file(task.stdout, 1, write_flags)
                #else:
                    #dup2devnull(2, os.O_WRONLY)

                if task.stderr is not None:
                    dup2file(task.stderr, 2, write_flags)
                #else:
                    #dup2devnull(2, os.O_WRONLY)

                if task.setpgrp:
                    os.setpgrp()

                if task.cwd:
                    os.chdir(task.cwd)

                args = task.args

                if isinstance(args, types.StringTypes):
                    args = [args]
                else:
                    args = list(args)

                if task.shell:
                    args = ["/bin/sh", "-c"] + ' '.join(args)
                    #if executable:
                        #args[0] = executable

                #if executable is None:
                    #executable = args[0]

                args = running_task.fix_args(args)
                logging_debug('+ args %s' % args)

                self._modify_signals(False)

                running_task.on_before_exec() # Must run after _modify_signals

                os.execvp(args[0], args)

            except BaseException as e:
                exit_code = 64
                try:
                    with os.fdopen(exec_err_wr, 'w') as out:
                        serialize(out, e)
                        out.flush()
                except:
                    exit_code = 65
            except:
                exit_code = 66

            os._exit(exit_code)

    def _process_send_signal_message(self, msg):
        with self._lock:
            err = None
            was_sent = False
            task = self._active.try_get_by_task_id(msg.process_task_id)
            if task:
                if task.exec_errored:
                    err = RuntimeError("Can't kill exec_errored tasks")
                else:
                    try:
                        task.send_signal(msg.sig, msg.group)
                        was_sent = True
                    except Exception as err:
                        print >>sys.stderr, "+ _process_send_signal_message:", err
                        pass
            else:
                pass # terminated

            self._send_queue.append(
                SendSignalResponseMessage(msg.task_id, was_sent, err))

            self._send_queue_not_empty.notify()

    def _process_new_process_message(self, msg):
        t0 = time.time()
        pid, error = self._start_process(msg)

        logging_debug('_start_process time %s' % (time.time() - t0))

        with self._lock:
            self._enqueue_start_msg(msg.task_id, pid, error)
            self._send_queue_not_empty.notify()

        logging_debug('after _enqueue_start_msg')

    @exit_on_error
    def _read_loop(self):
        set_thread_name('rem-RunnerSrvRd')

        channel_in = self._channel_in
        stop_request_received = False

        logging_debug('_Server._read_loop started')

        while True:
            try:
                msg = deserialize(channel_in)
            except EOFError:
                logging_debug('_Server._read_loop EOFError')
                break

            if isinstance(msg, NewTaskParamsMessage):
                if stop_request_received:
                    raise RuntimeError("Message in channel after StopServiceRequestMessage")
                self._process_new_process_message(msg)

            elif isinstance(msg, SendSignalRequestMessage):
                self._process_send_signal_message(msg)

            elif isinstance(msg, StopServiceRequestMessage):
                stop_request_received = True
                with self._lock:
                    self._should_stop = True
                    self._send_queue_not_empty.notify()

            else:
                raise RuntimeError('Unknown message type')

        if not stop_request_received:
            raise RuntimeError('Socket closed without StopServiceRequestMessage message')

        self._channel.shutdown(socket.SHUT_RD)
    # TODO
        self._read_stopped = True
        logging_debug('_Server._read_loop finished')

    @exit_on_error
    def _write_loop(self):
        set_thread_name('rem-RunnerSrvWr')

        channel_out = self._channel_out
        send_queue = self._send_queue

        logging_debug('_Server._write_loop started')

        while True:
            last_iter = False

            with self._lock:
                while not send_queue and not (self._should_stop and not self._active):
                    self._send_queue_not_empty.wait()

                messages = []
                while send_queue:
                    messages.append(send_queue.popleft())

                if self._should_stop and not self._active:
                    last_iter = True
                    logging_debug('_Server._write_loop append(StopServiceResponseMessage)')
                    messages.append(StopServiceResponseMessage())

            for msg in messages:
                serialize(channel_out, msg)
            channel_out.flush()

            if last_iter:
                break

        self._channel.shutdown(socket.SHUT_WR)
        logging_debug('_Server._write_loop finished')

    # TODO
        self._write_stopped = True

    def _enqueue_start_msg(self, task_id, pid, error):
        task = None
        if pid:
            task = self._active.get_by_pid(pid)

        terminated = task and task.exit_status is not None

        if terminated:
            self._active.pop_by_pid(pid)

        if error:
            pid = None

        self._send_queue.append(
            ProcessStartMessage(task_id, pid, error))

        if not task: # failed before/on fork
            return

        task.start_sent = True

        if terminated and not task.exec_errored:
            self._send_queue.append(
                ProcessTerminationMessage(task_id, task.exit_status))

    def _enqueue_term_msg(self, pid, exit_status):
        task = self._active.get_by_pid(pid)

        task.on_process_termination(exit_status)

        if task.start_sent:
            self._active.pop_by_pid(pid)

            if not task.exec_errored:
                self._send_queue.append(
                    ProcessTerminationMessage(task.task_id, task.exit_status))

        # SIGCHLD before _send_queue.append(ProcessStartMessage(
        else:
            pass

    def _sig_chld_handler(self, signum, frame):
        # http://bugs.python.org/issue9535
        if os.getpid() != self._sig_chld_handler_pid:
            return

        with self._lock:
            while self._running_count:
                pid, status = os.waitpid(-1, os.WNOHANG)

                if pid == 0:
                    break

                #logging_debug('_Server._sig_chld_handler pid = %d, status = %d' % (pid, status))

                self._running_count -= 1
                self._enqueue_term_msg(pid, status)

            self._send_queue_not_empty.notify()


def _handle_exitstatus(sts, _WIFSIGNALED=os.WIFSIGNALED,
        _WTERMSIG=os.WTERMSIG, _WIFEXITED=os.WIFEXITED,
        _WEXITSTATUS=os.WEXITSTATUS):
    if _WIFSIGNALED(sts):
        return -_WTERMSIG(sts)
    elif _WIFEXITED(sts):
        return _WEXITSTATUS(sts)
    else:
        raise RuntimeError("Unknown child exit status!")


class _Popen(object):
    def __init__(self, pid, task_id, exit_status, client):
        self.pid = pid
        self._task_id = task_id
        self._exit_status = exit_status
        self._client = client

    def __repr__(self):
        self.poll()
        cls = type(self)
        ret = '<%s.%s: pid=%s, ' % (cls.__module__, cls.__name__, self.pid)
        ret += 'returncode=%s' % self._exit_status.get_raw() \
            if self._exit_status.is_set() \
            else 'running'
        ret += '>'
        return ret

    def wait_no_throw(self, timeout=None, deadline=None):
        if self._exit_status.is_set():
            return True

        if timeout is not None:
            pass
        elif deadline is not None:
            timeout = deadline - time.time()

        if not self._exit_status.wait(timeout):
            return False

        return True

    def wait(self, timeout=None, deadline=None):
        self.wait_no_throw(timeout, deadline)
        return self.returncode

    def is_terminated(self):
        return self._exit_status.is_set()

    @property
    def returncode(self):
        if self._exit_status.is_set():
            return self._exit_status.get()
        return None

    def poll(self):
        return self.wait(timeout=0)

    def check(self):
        check_retcode(self.wait(), '#TODO args')

    def send_signal_safe(self, sig, group=False):
        return self._client._send_signal(self, sig, group)

    #def send_signal(self, sig, group=False):
        #_send_signal(self.pid, sig, group)

    #def terminate(self, group=False):
        #self.send_signal(signal.SIGTERM, group)

    #def kill(self):
        #self.send_signal(signal.SIGKILL)

    #def killpg(self):
        #self.send_signal(signal.SIGKILL, group=True)

    #def terminate_safe(self, group=False):
        #return self.send_signal_safe(signal.SIGTERM, group)

    #def kill_safe(self):
        #return self.send_signal_safe(signal.SIGKILL)

    #def killpg_safe(self):
        #return self.send_signal_safe(signal.SIGKILL, group=True)

    def communicate(self):
        raise NotImplementedError("communicate not implemented")

    def pipe_cloexec(self):
        raise NotImplementedError("pipe_cloexec not implemented")


def _weak_method(m):
    obj = weakref.proxy(m.__self__)
    func = m.__func__

    def run():
        return func(obj)

    return run


# TODO Use __slots__
class Bool(object):
    def __init__(self):
        self.__state = False

    def set(self, value=True):
        self.__state = value

    def unset(self):
        self.__state = False

    def __nonzero__(self):
        return self.__state

    def __str__(self):
        return str(self.__state)

    def __repr__(self):
        return repr(self.__state)


def fail_on_error(func):
    def impl(self):
        fail = self._fail
        try:
            func(self)
        except:
            fail()
            raise

    impl.__name__ = func.__name__

    return impl


# TODO Use another approach for weakness:
# wrapper class for ref-counting and __del__,
# and impl, which will be passed with strong refs to threads
# Proxy.__del__(): self._impl.stop()
class _Client(object):
    class RunTask(object):
        def __init__(self):
            self.pid = Promise()
            self.term_info = Promise()

        def list_promises(self):
            return [self.pid, self.term_info]

    class SignalTask(object):
        def __init__(self):
            self.ack = Promise()

        def list_promises(self):
            return [self.ack]

    def __init__(self, server_pid, channel): # executor_pid, executor_stderr
        self._server_pid = server_pid
        self._server_exit_status = None
        self._channel = channel

        self._channel_in  = dupfdopen(channel, 'r')
        self._channel_out = dupfdopen(channel, 'w')

        self._errored = Bool()
        self._input_queue = deque()
        self._lock = threading.Lock()
        self._queue_not_empty = threading.Condition(self._lock)
        self._should_stop = Bool()
        self._next_task_id = 1
        self._tasks = {}

        self._fail = self._create_fail()

        self._server_stop = Promise()

        self._read_thread_inited = threading.Event()
        self._write_thread_inited = threading.Event()

        write_thread = ProfiledThread(
            target=_weak_method(self._write_loop),
            name_prefix='RunnerClnWr')

        self._write_thread = weakref.ref(write_thread)

        read_thread = ProfiledThread(
            target=_weak_method(self._read_loop),
            name_prefix='RunnerClnRd')

        self._read_thread = weakref.ref(read_thread)

        write_thread.daemon = True
        read_thread.daemon = True

        write_thread.start()
        read_thread.start()

        self._read_thread_inited.wait()
        self._write_thread_inited.wait()

    def _create_fail(self):
        errored = self._errored
        queue_not_empty = self._queue_not_empty
        channel = self._channel
        tasks = self._tasks

        # Don't closure on self to prevent cycle
        def fail():
            with queue_not_empty:
                if errored:
                    return

                errored.set()

                try:
                    channel.shutdown(socket.SHUT_RDWR)
                except:
                    pass

                exc = ServiceUnavailable("Runner abnormal termination")

                tasks_values = tasks.values()
                tasks.clear()

                for task in tasks_values:
                    for p in task.list_promises():
                        if not p.is_set():
                            try:
                                p.set(None, exc)
                            except:
                                pass

                queue_not_empty.notify_all()

        return fail

    def _wait_stop(self):
        for w in [self._read_thread, self._write_thread]:
            t = w()
            if t:
                t.join()
        _, status = os.waitpid(self._server_pid, 0)
        logging.info('_Server exited with %s' % status)
        if status:
            raise RuntimeError("Runner.Server process exited abnormally %d" % status)

    def stop(self):
        do_wait = False

        with self._lock:
            if not self._should_stop:
                self._should_stop.set()
                self._queue_not_empty.notify()
                do_wait = True

        if do_wait:
            self._server_stop.run_and_set(self._wait_stop)

        self._server_stop.to_future().get()

    def __del__(self):
        self.stop()

    def _get_next_task_id(self):
        ret = self._next_task_id
        self._next_task_id += 1
        return ret

    def _enqueue(self, task, msg):
        with self._lock:
            if self._errored:
                raise ServiceUnavailable("Runner in malformed state")

            task_id = self._get_next_task_id()

            msg.task_id = task_id
            self._tasks[task_id] = task

            self._input_queue.append(msg)
            self._queue_not_empty.notify()

            return task_id

    def start(self, *pargs, **pkwargs):
        task = self.RunTask()
        task_id = self._enqueue(task, NewTaskParamsMessage(*pargs, **pkwargs))

        def join_pid():
            pid = task.pid.to_future().get()

            return _Popen(
                pid=pid,
                task_id=task_id,
                exit_status=task.term_info.to_future(),
                client=self # FIXME weak
            )

        return join_pid

    def Popen(self, *pargs, **pkwargs):
        def start():
            return self.start(*pargs, **pkwargs)()

        stdin_content = pkwargs.pop('stdin_content', None)

        if stdin_content is None:
            return start()

    # FIXME Why 'stdin_content' supported only in Popen?
    # TODO Reimplement: use named pipes?
        with NamedTemporaryFile() as tmp:
            tmp.write(stdin_content)
            tmp.flush()

            pkwargs['stdin'] = tmp.name

            return start()

    def _send_signal(self, target_task, sig, group):
        task = self.SignalTask()
        self._enqueue(task, SendSignalRequestMessage(target_task._task_id, sig, group))
        return task.ack.to_future()

    @fail_on_error
    def _write_loop(self):
        queue = self._input_queue
        channel_out = self._channel_out
        lock = self._lock
        queue_not_empty = self._queue_not_empty
        channel = self._channel
        should_stop = self._should_stop
        errored = self._errored
        self._write_thread_inited.set()
        del self # don't use self

        logging.debug('_Client._write_loop started')

        while True:
            with lock:
                while not (should_stop or queue or errored):
                    queue_not_empty.wait()

                if errored:
                    raise RuntimeError("Errored in another thread")

                messages = []
                while queue:
                    messages.append(queue.popleft())

                if should_stop:
                    messages.append(StopServiceRequestMessage())
                    logging.debug('_Client._write_loop append(StopServiceRequestMessage)')

            for msg in messages:
                serialize(channel_out, msg)
            channel_out.flush()

            if should_stop:
                break

        channel.shutdown(socket.SHUT_WR)

        logging.debug('_Client._write_loop finished')

    @fail_on_error
    def _read_loop(self):
        channel_in = self._channel_in
        lock = self._lock
        tasks = self._tasks
        should_stop = self._should_stop
        channel = self._channel
        errored = self._errored
        self._read_thread_inited.set()
        del self # don't use self

        stop_response_received = False

        logging.debug('_Client._read_loop started')

        while True:
            try:
                msg = deserialize(channel_in)
            except EOFError:
                logging.debug('_Client._read_loop EOFError')
                break

            #logging.debug('_Client._read_loop %s' % msg)

            if isinstance(msg, ProcessStartMessage):
                #logging.debug('_Client._read_loop ProcessStartMessage for task_id=%d, pid=%d, received'  % (msg.task_id, msg.pid))

                pid, error = msg.pid, msg.error

                if error:
                    #error = RuntimeError(error) # FIXME pickle Exception?
                    pass # TODO Check! :)

                with lock:
                    try:
                        task = tasks[msg.task_id] if pid else tasks.pop(msg.task_id)
                    except KeyError:
                        if errored:
                            continue
                        else:
                            raise
                    task.pid.set(pid, error)

            elif isinstance(msg, ProcessTerminationMessage):
                #logging.info('_Client._read_loop ProcessTerminationMessage for task_id=%d, received'  % msg.task_id)

                with lock:
                    try:
                        task = tasks.pop(msg.task_id)
                    except KeyError:
                        if errored: # FIXME Don't remember
                            continue
                        else:
                            raise

            # FIXME Better?
                p = task.term_info
                if isinstance(msg.exit_status, Exception):
                    p.set(None, msg.exit_status)
                else:
                    p.set(msg.exit_status)

            elif isinstance(msg, SendSignalResponseMessage):
                with lock:
                    try:
                        task = tasks.pop(msg.task_id)
                    except KeyError:
                        if errored:
                            continue
                        else:
                            raise

                task.ack.set(msg.was_sent, msg.error)

            elif isinstance(msg, StopServiceResponseMessage):
                logging.debug('_Client._read_loop StopServiceResponseMessage received')

                if not should_stop: # FIXME more fine check
                    raise RuntimeError('StopServiceResponseMessage received but StopServiceRequestMessage was not send')
                if tasks:
                    raise RuntimeError('StopServiceResponseMessage received but not for all process replices was received')

                stop_response_received = True

            else:
                raise RuntimeError('Unknown message type')

        if not stop_response_received:
            raise RuntimeError('Socket closed without StopServiceResponseMessage')

        channel.shutdown(socket.SHUT_RD)
        logging.debug('_Client._read_loop finished')


def dup2file(filename, newfd, flags, mode=0666):
    oldfd = os.open(filename, flags, mode)
    os.dup2(oldfd, newfd)
    os.close(oldfd)


def dup2devnull(newfd, flags):
    dup2file('/dev/null', newfd, flags)


def _close_fds(white_list):
    for fd in xrange(3, MAXFD):
        if fd not in white_list:
            try:
                os.close(fd)
            except:
                pass


def _run_executor(child_err_wr, channel, pgrpguard_binary=None):
    _close_fds([channel.fileno(), child_err_wr])

    dup2devnull(0, os.O_RDONLY)
    #dup2devnull(1, os.O_WRONLY) # FIXME

    #os.dup2(child_err_wr, 2) # FIXME
    os.close(child_err_wr)

    ex = _Server(channel, pgrpguard_binary)
    ex.wait()


def _run_executor_wrapper(*args):
    try:
        _run_executor(*args)
        exit_code = 0
    except Exception as e:
        try:
            os.write(2, "Failed to execute child function: %s\n" % e)
        except:
            pass
        exit_code = 1
    os._exit(exit_code)


def Runner(pgrpguard_binary=None):
    child_err_rd, child_err_wr = os.pipe()
    channel_parent, channel_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)

    pid = os.fork()

    if pid:
        os.close(child_err_wr)
        channel_child.close()
        del channel_child
        logging.info("_Server pid = %s" % pid)
        return _Client(pid, channel_parent) # child_err_rd
    else:
        try:
            channel_parent.close()
            del channel_parent
            os.close(child_err_rd)
        except BaseException as e:
            try:
                os.write(2, '%s\n' % e)
            except:
                pass
            os._exit(1)
        set_thread_name("rem-RunnerSrv")
        _run_executor_wrapper(child_err_wr, channel_child, pgrpguard_binary)


class RunnerPool(object):
    def __init__(self, size, pgrpguard_binary=None):
        self._good = [Runner(pgrpguard_binary) for _ in xrange(size)]
        self._bad  = []
        self._counter = 0
        self._lock = threading.Lock()

    def _get_impl(self):
        with self._lock:
            if not self._good:
                raise ServiceUnavailable()
            self._counter += 1
            return self._good[self._counter % len(self._good)]

    def _do(self, f):
        while True:
            impl = self._get_impl()

            try:
                return f(impl)
            except ServiceUnavailable:
                with self._lock:
                    try:
                        self._good.remove(impl)
                    except ValueError:
                        pass
                    else:
                        self._bad.append(impl)

    def Popen(self, *args, **kwargs):
        def do(impl):
            return impl.Popen(*args, **kwargs)
        return self._do(do)

    def start(self, *args, **kwargs):
        def do(impl):
            return impl.start(*args, **kwargs)
        return self._do(do)

    def stop(self):
        self._good = []
        self._bad  = []


class RunnerPoolNaive(object):
    def __init__(self, size, pgrpguard_binary=None):
        self._good = [Runner(pgrpguard_binary) for _ in xrange(size)]
        self._counter = 0

    def _get_impl(self):
        self._counter += 1
        return self._good[self._counter % len(self._good)]

    def Popen(self, *args, **kwargs):
        return self._get_impl().Popen(*args, **kwargs)

    def start(self, *args, **kwargs):
        return self._get_impl().start(*args, **kwargs)

    def stop():
        self._good = []


class ScopedVal(object):
    def __init__(self, val):
        self._val = val

    def __enter__(self):
        return self._val

    def __exit__(self, *args):
        pass


class BrokenRunner(object):
    def Popen(self, *args, **kwargs):
        raise ServiceUnavailable()

    def start(self, *args, **kwargs):
        raise ServiceUnavailable()

    def stop():
        pass


def _Popen_fallback(*args, **kwargs):
    logging.error("using fallback # TODO")

    stdin_content = kwargs.pop('stdin_content', None)

    task = NewTaskParamsMessage(*args, **kwargs)

    def file_or_none(file):
        if file is None:
            return ScopedVal(None)
        elif isinstance(file, str):
            return open(file, 'r')
        else:
            raise ValueError()

    class NamedTemporaryFileWithContent(object):
        def __init__(self, content):
            self._file = NamedTemporaryFile()
            self._file.write(content)
            self._file.flush()

        def __enter__(self):
            return self._file.__enter__()

        def __exit__(self, *args):
            self._file.__exit__(*args)

    stdin_mgr = NamedTemporaryFileWithContent(stdin_content) \
        if stdin_content is not None \
        else file_or_none(task.stdin)

    with stdin_mgr as stdin:
        with file_or_none(task.stdout) as stdout:
            with file_or_none(task.stderr) as stderr:
                refl=dict(
                    args=task.args,
                    stdin=stdin,
                    stdout=stdout,
                    stderr=stderr,
                    preexec_fn=os.setpgrp if task.setpgrp else None,
                    cwd=task.cwd,
                    shell=task.shell,
                    close_fds=True, # as in _Server
                )
                logging.error(repr(refl))

                return subprocess.Popen(**refl)


class FallbackkedRunner(object):
    def __init__(self, backend):
        self._backend = backend
        self._broken = False

    def Popen(self, *args, **kwargs):
        if self._broken:
            return _Popen_fallback(*args, **kwargs)

        try:
            return self._backend.Popen(*args, **kwargs)
        except ServiceUnavailable:
            self._broken = True
            return _Popen_fallback(*args, **kwargs)

    def start(self, *args, **kwargs):
        raise NotImplementedError() # FIXME

    def stop(self):
        self._backend.stop()


DEFAULT_RUNNER = None


def ResetDefaultRunner(pool_size=1, pgrpguard_binary=None, runner=None):
    global DEFAULT_RUNNER

    if runner:
        DEFAULT_RUNNER = runner
        return

    if pool_size <= 0:
        raise ValueError("pool_size must be greater than 0");

    DEFAULT_RUNNER = None

    DEFAULT_RUNNER = Runner(pgrpguard_binary) if pool_size == 1 \
        else RunnerPool(pool_size, pgrpguard_binary)


def DestroyDefaultRunner():
    global DEFAULT_RUNNER
    DEFAULT_RUNNER = None


atexit.register(DestroyDefaultRunner)


def Popen(*args, **kwargs):
    return DEFAULT_RUNNER.Popen(*args, **kwargs)


def start(*args, **kwargs):
    return DEFAULT_RUNNER.start(*args, **kwargs)


# XXX Copy-pasted from subprocess.py

def call(*args, **kwargs):
    return Popen(*args, **kwargs).wait()


def check_call(*args, **kwargs):
    retcode = call(*args, **kwargs)
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = args[0]
        check_retcode(retcode, cmd)
    return 0


def check_retcode(retcode, cmd):
    if retcode:
        raise CalledProcessError(retcode, cmd)


def check_output(*args, **kwargs):
    raise NotImplementedError('check_output not implemented')

