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
from subprocess import CalledProcessError, MAXFD
import types
import errno
import weakref

from future import Promise
from profile import ProfiledThread

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
    def __init__(self, args, stdin=None, stdin_content=None, stdout=None, stderr=None, setpgrp=False, cwd=None, shell=False):
        self.task_id = None
        self.args = args
        self.stdin = stdin # string or None
        self.stdin_content = stdin_content # string or None
        self.stdout = stdout # filename or None
        self.stderr = stderr # filename or None
        self.setpgrp = setpgrp
        self.cwd = cwd # filename or None
        self.shell = shell # filename or None

def set_cloexec(fd):
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

class _Server(object):
    def __init__(self, channel):
        self._sig_chld_handler_pid = os.getpid() # #9535
        signal.signal(signal.SIGCHLD, self._sig_chld_handler)
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, signal.SIG_IGN)

        for sig in [signal.SIGCHLD, signal.SIGINT, signal.SIGTERM]:
            signal.siginterrupt(sig, False)

        self._channel = channel
        set_cloexec(self._channel)

        self._channel_in = dupfdopen(channel, 'r')
        set_cloexec(self._channel_in)

        self._channel_out = dupfdopen(channel, 'w')
        set_cloexec(self._channel_out)

        self._should_stop = False
        self._lock = threading.Lock()
        self._active = {}
        self._running_count = 0
        self._send_queue = deque()
        self._send_queue_not_empty = threading.Condition(self._lock)
        self._read_thread = ProfiledThread(target=self._read_loop, name_prefix='RunnerSrvRd')
        self._write_thread = ProfiledThread(target=self._write_loop, name_prefix='RunnerSrvWr')
        self._read_thread.start()
        self._write_thread.start()

# FIXME
        self._read_stopped = False
        self._write_stopped = False

    def wait(self):
# Fuck Python
        while not(self._read_stopped and self._write_stopped):
            self._read_thread.join(1) # sleeps for 50ms actually
            self._write_thread.join(1)
        self._read_thread.join()
        self._write_thread.join()

    class RunningTask(object):
        def __init__(self, task_id):
            self.task_id = task_id
            self.start_sent = False
            self.exit_status = None
            self.exec_errored = False

    def _start_process(self, task):
        exec_err_rd, exec_err_wr = os.pipe()

        running_task = None
        try:
            t0 = time.time()
            with self._lock:
                pid = os.fork()
                if pid:
                    running_task = self._active[pid] = self.RunningTask(task.task_id)
                    self._running_count += 1
            _fork_time = time.time() - t0
        except OSError as e:
            return None, str(e)

        if pid:
            logging.debug('fork time %s' % _fork_time)

        if pid:
            os.close(exec_err_wr)
            exec_error = None
            try:
                with os.fdopen(exec_err_rd) as in_:
                    s = in_.read()
                    if s:
                        exec_error = pickle.loads(s)
            except BaseException as exec_error:
                try:
                    os.close(exec_err_rd)
                except:
                    pass

            running_task.exec_errored = bool(exec_error)
            return pid, exec_error or None

        else:
            exit_code = 0
            try:
                set_cloexec(exec_err_wr)
                os.close(exec_err_rd)

                if task.stdin is not None:
                    dup2file(task.stdin, 0, os.O_RDONLY)
                elif task.stdin_content is not None:
                    with NamedTemporaryFile() as tmp: # TODO Reimplement
                        tmp.write(task.stdin_content)
                        tmp.flush()
                        dup2file(tmp.name, 0, os.O_RDONLY)
                #else:
                    #dup2devnull(0, os.O_RDONLY)

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
                    args = ["/bin/sh", "-c"] + args
                    #if executable:
                        #args[0] = executable

                #if executable is None:
                    #executable = args[0]

                os.execvp(args[0], args)

            except BaseException as e:
                exit_code = 1
                try:
                    with os.fdopen(exec_err_wr, 'w') as out:
                        serialize(out, e)
                        out.flush()
                except:
                    pass
            except:
                exit_code = 1
            os._exit(exit_code)

    @exit_on_error
    def _read_loop(self):
        channel_in = self._channel_in
        stop_request_received = False

        logging.debug('_Server._read_loop started')

        while True:
            try:
                msg = deserialize(channel_in)
            except EOFError:
                logging.debug('_Server._read_loop EOFError')
                break

            if isinstance(msg, NewTaskParamsMessage):
                #logging.debug('_Server._read_loop NewTaskParamsMessage received')

                if stop_request_received:
                    raise RuntimeError("Message in channel after StopServiceRequestMessage")

                t0 = time.time()
                pid, error = self._start_process(msg)
                with self._lock:
                    self._enqueue_start_msg(msg.task_id, pid, error)
                    self._send_queue_not_empty.notify()
                logging.debug('_start_process time %s' % (time.time() - t0))

            elif isinstance(msg, StopServiceRequestMessage):
                #logging.debug('_Server._read_loop StopServiceRequestMessage received')
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
        logging.debug('_Server._read_loop finished')

    @exit_on_error
    def _write_loop(self):
        channel_out = self._channel_out
        send_queue = self._send_queue

        logging.debug('_Server._write_loop started')

        while True:
            last_iter = False

            with self._lock:
                while not send_queue and not (self._should_stop and not self._active):
                    #logging.info('before self._send_queue_not_empty.wait: %r' % dict(
                        #send_queue=len(send_queue), should_stop=self._should_stop,
                        #active=len(self._active), running=self._running_count,
                    #))
                    self._send_queue_not_empty.wait()

                messages = []
                while send_queue:
                    messages.append(send_queue.popleft())

                if self._should_stop and not self._active:
                    last_iter = True
                    logging.info('_Server._write_loop append(StopServiceResponseMessage)')
                    messages.append(StopServiceResponseMessage())

            for msg in messages:
                serialize(channel_out, msg)
            channel_out.flush()

            if last_iter:
                break

        self._channel.shutdown(socket.SHUT_WR)
        logging.debug('_Server._write_loop finished')

    # TODO
        self._write_stopped = True

    def _enqueue_start_msg(self, task_id, pid, error):
        task = None
        if pid:
            task = self._active[pid]

        terminated = task and task.exit_status is not None

        if terminated:
            self._active.pop(pid)

        if error:
            pid = None

        self._send_queue.append(
            ProcessStartMessage(task_id, pid, error))

        task.start_sent = True

        if terminated and not task.exec_errored:
            self._send_queue.append(
                ProcessTerminationMessage(task_id, task.exit_status))

    def _enqueue_term_msg(self, pid, exit_status):
        task = self._active[pid]

        if task.start_sent:
            self._active.pop(pid)
            if not task.exec_errored:
                self._send_queue.append(
                    ProcessTerminationMessage(task.task_id, exit_status))
        else:
            task.exit_status = exit_status

    def _sig_chld_handler(self, signum, frame):
        # http://bugs.python.org/issue9535
        if os.getpid() != self._sig_chld_handler_pid:
            #os.write(2, 'WRONG_PROCESS_SIG_CHLD\n')
            return

        with self._lock:
            while self._running_count:
                pid, status = os.waitpid(-1, os.WNOHANG)

                if pid == 0:
                    break

                #logging.debug('_Server._sig_chld_handler pid = %d, status = %d' % (pid, status))

                self._running_count -= 1
                self._enqueue_term_msg(pid, status)

            self._send_queue_not_empty.notify()


class _Popen(object):
    def __init__(self, pid, exit_status): #, send_signal):
        self.pid = pid
        self._exit_status = exit_status
        #self._send_signal = send_signal
        self.returncode = None

    def _handle_exitstatus(self, sts, _WIFSIGNALED=os.WIFSIGNALED,
            _WTERMSIG=os.WTERMSIG, _WIFEXITED=os.WIFEXITED,
            _WEXITSTATUS=os.WEXITSTATUS):
        if _WIFSIGNALED(sts):
            return -_WTERMSIG(sts)
        elif _WIFEXITED(sts):
            return _WEXITSTATUS(sts)
        else:
            # Should never happen
            raise RuntimeError("Unknown child exit status!")

    def _poll(self, timeout=None, deadline=None):
        if self.returncode is not None:
            return self.returncode

        if timeout is not None:
            pass
        elif deadline is not None:
            timeout = deadline - time.time()

        if not self._exit_status.wait(timeout):
            return None

        self.returncode = self._handle_exitstatus(self._exit_status.get())
        self._exit_status = None

        return self.returncode

    def poll(self):
        return self._poll(timeout=0)

    def wait(self, timeout=None, deadline=None):
        return self._poll(timeout=timeout, deadline=deadline)

    def send_signal(self, sig):
        os.kill(self.pid, sig)

    def terminate(self):
        self.send_signal(signal.SIGTERM)

    def kill(self):
        self.send_signal(signal.SIGKILL)

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
        try:
            func(self)
        except:
            self._fail()
            raise

    impl.__name__ = func.__name__

    return impl


class _Client(object):
    class Task(object):
        def __init__(self):
            self.pid = Promise()
            self.term_info = Promise()

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

        def create_fail():
            errored = self._errored
            queue_not_empty = self._queue_not_empty
            channel = self._channel
            tasks = self._tasks

            def fail():
                with queue_not_empty:
                    if errored:
                        return

                    errored.set()

                    try:
                        channel.shutdown(socket.SHUT_RDWR)
                    except:
                        pass

                    exc = RuntimeError("Runner abnormal termination")

                    for task in tasks.itervalues():
                        for p in [task.pid, task.term_info]:
                            if not p.is_set():
                                try:
                                    p.set(None, exc)
                                except:
                                    pass

                    queue_not_empty.notify_all()

            return fail

        self._fail = create_fail()

        self._server_stop = Promise()

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

    def _wait_stop(self):
        for w in [self._write_thread, self._write_thread]:
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

        self._server_stop.get_future().get()

    def __del__(self):
        self.stop()

    def start(self, *pargs, **pkwargs):
        with self._lock:
            if self._errored:
                raise RuntimeError("Runner in malformed state")

            task_id = self._next_task_id
            self._next_task_id += 1

            msg = NewTaskParamsMessage(*pargs, **pkwargs)
            msg.task_id = task_id

            task = _Client.Task()
            self._tasks[task_id] = task

            self._input_queue.append(msg)
            self._queue_not_empty.notify()

        def join_pid():
            pid = task.pid.get_future().get()

            return _Popen(
                pid=pid,
                exit_status=task.term_info.get_future(),
            )

        return join_pid

    def Popen(self, *pargs, **pkwargs):
        return self.start(*pargs, **pkwargs)()

    @fail_on_error
    def _write_loop(self):
        queue = self._input_queue
        channel_out = self._channel_out
        lock = self._lock
        queue_not_empty = self._queue_not_empty
        channel = self._channel
        should_stop = self._should_stop
        errored = self._errored
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

                #with lock:
                task = tasks[msg.task_id] if pid else tasks.pop(msg.task_id)

                if error:
                    error = RuntimeError(error) # FIXME pickle Exception?

                task.pid.set(pid, error)

            elif isinstance(msg, ProcessTerminationMessage):
                #logging.info('_Client._read_loop ProcessTerminationMessage for task_id=%d, received'  % msg.task_id)

                #with lock:
                task = tasks.pop(msg.task_id)
                task.term_info.set(msg.exit_status)

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


def dup2file(filename, newfd, flags):
    oldfd = os.open(filename, flags)
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

def _run_executor(child_err_wr, channel):
    _close_fds([channel.fileno(), child_err_wr])

    dup2devnull(0, os.O_RDONLY)
    #dup2devnull(1, os.O_WRONLY) # FIXME

    #os.dup2(child_err_wr, 2) # FIXME
    os.close(child_err_wr)

    ex = _Server(channel)
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

def Runner():
    child_err_rd, child_err_wr = os.pipe()
    channel_parent, channel_child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM, 0)

    pid = os.fork()

    if pid:
        os.close(child_err_wr)
        channel_child.close()
        del channel_child
        logging.debug("_Server pid = %s" % pid)
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
        _run_executor_wrapper(child_err_wr, channel_child)

class RunnerPool(object):
    def __init__(self, size):
        self._impls = [Runner() for _ in xrange(size)]
        self._counter = 0

# bullshit
    def _get_impl(self):
        self._counter += 1
        return self._impls[self._counter % len(self._impls)]

    def start(self, *args, **kwargs):
        return self._get_impl().start(*args, **kwargs)

    def Popen(self):
        return self._get_impl().Popen(*args, **kwargs)

    def stop(self):
        last_exc = None
        while len(self._impls):
            impl = self._impls.pop()
            try:
                impl.stop()
            except Exception as e:
                last_exc = e
        if e:
            raise e

DEFAULT_RUNNER = None

def CreateDefaultRunner():
    global DEFAULT_RUNNER

    if DEFAULT_RUNNER:
        raise RuntimeError()

    DEFAULT_RUNNER = Runner()

def Popen(*args, **kwargs):
    return DEFAULT_RUNNER.Popen(*args, **kwargs)

def start(*args, **kwargs):
    return DEFAULT_RUNNER.start(*args, **kwargs)

def DestroyDefaultRunnerIfNeed():
    global DEFAULT_RUNNER
    if DEFAULT_RUNNER:
        DEFAULT_RUNNER.stop()
        DEFAULT_RUNNER = None

# XXX Copy-pasted from subprocess.py

def call(*args, **kwargs):
    return Popen(*args, **kwargs).wait()

def check_call(*args, **kwargs):
    retcode = call(*args, **kwargs)
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = args[0]
        raise CalledProcessError(retcode, cmd)
    return 0

def check_output(*args, **kwargs):
    raise NotImplementedError('check_output not implemented')

