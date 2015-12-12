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
import atexit

from future import Promise
from profile import ProfiledThread

#class Messages(object):
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
    def __init__(self, args, stdin=None, stdin_content=None, stdout=None, stderr=None, setpgrp=False, cwd=None):
        self.task_id = None
        self.args = args
        self.stdin = stdin # string or None
        self.stdin_content = stdin_content # string or None
        self.stdout = stdout # filename or None
        self.stderr = stderr # filename or None
        self.setpgrp = setpgrp
        self.cwd = cwd # filename or None

def set_cloexec(fd):
    fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC | fcntl.fcntl(fd, fcntl.F_GETFD))

def set_nonblock(fd):
    fcntl.fcntl(fd, fcntl.F_SETFL, os.O_NONBLOCK | fcntl.fcntl(fd, fcntl.F_GETFL))

def noeintr(f):
    while True:
        try:
            return f()
        except OSError as e:
            if e.errno != errno.EINTR:
                raise
        else:
            break

'''
class PipeEvent(object):
    def __init__(self):
        self._rd, self._wr = os.pipe()
        set_nonblock(self._wr)

    @property
    def fd(self):
        return self._rd

    def notify(self):
        while True:
            try:
                os.write(self._wr, '\000')
                break
            except OSError as e:
                if e.errno == errno.EINTR:
                    continue
                elif e.errno == errno.EAGAIN:
                    break
                else:
                    raise

    def wait(self):
        while True:
            try:
                data = os.read(self._rd, 4096)
                assert len(data)
                break
            except OSError as e:
                if e.errno == errno.EINTR:
                    continue
                else:
                    raise
'''

def serialize(stream, data):
    return pickle.dump(data, stream, pickle.HIGHEST_PROTOCOL)

def deserialize(stream):
    return pickle.load(stream)

#class MsgWriter(object):
    #def __init__(self, data)
        #self.msg = serialize_to_string(data)
        #self.msg_len_str = serialize_to_string(len(self.msg))
        #self.len_

class _Executor(object):
    def __init__(self, channel):
        signal.signal(signal.SIGCHLD, self._sig_chld_handler)
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, signal.SIG_IGN)

        for sig in [signal.SIGCHLD, signal.SIGINT, signal.SIGTERM]:
            signal.siginterrupt(sig, False)

        self._channel = channel
        self._channel_out = os.fdopen(channel.fileno(), 'w') # FIXME Will throw
        self._channel_in  = os.fdopen(channel.fileno(), 'r')
        set_cloexec(self._channel.fileno())
        self._should_stop = False
        self._lock = threading.Lock()
        self._running = {}
        self._running_not_empty = threading.Condition(self._lock)
        self._send_queue = deque()
        self._send_queue_not_empty = threading.Condition(self._lock)
        self._read_thread = ProfiledThread(target=self._read_loop, name_prefix='RunnerSrvRd')
        self._write_thread = ProfiledThread(target=self._write_loop, name_prefix='RunnerSrvWr')
        self._read_thread.start()
        self._write_thread.start()

# TODO
        self._read_stopped = False
        self._write_stopped = False

    def __del__(self):
        try:
            self._channel_out.close()
        except:
            pass
        try:
            self._channel_in.close()
        except:
            pass

    def wait(self):
# TODO
        #logging.debug('_Executor.wait started')
# Fuck Python
        while not(self._read_stopped and self._write_stopped):
            self._read_thread.join(1) # sleeps for 50ms actually
            self._write_thread.join(1)
        self._read_thread.join()
        self._write_thread.join()

# TODO TODO TODO REFACTOR
    def _start_process(self, task):
        exec_err_rd, exec_err_wr = os.pipe()

        logging.debug('RUN %r' % task.__dict__)
        os.write(2, 'RUN %r\n' % task.__dict__)

        try:
            #logging.debug('before_fork')
            pid = os.fork()
            #if pid:
                #logging.debug('after_fork')
        except OSError as e:
            return None, str(e)

        if pid: # TODO
            self._running[pid] = task.task_id # TODO

        if pid:
            os.close(exec_err_wr)
            exec_error = None
            try:
                #in_ = os.fdopen(exec_err_rd)
                #exec_error = pickle.loads(in_.read())
                ##in_.close()
                #del in_

                with os.fdopen(exec_err_rd) as in_:
                    s = in_.read()
                    if s:
                        exec_error = pickle.loads(s)

                #str = os.read(exec_err_rd, 4096)
                #exec_error = pickle.loads(str) if str else None
            except BaseException as exec_error:
                try:
                    os.close(exec_err_rd)
                except:
                    pass
            return pid, exec_error or None

        else:
            code = 0
            try:
                set_cloexec(exec_err_wr)
                os.close(exec_err_rd)

                if task.stdin is not None:
                    dup2file(task.stdin, 0, os.O_RDONLY)
                elif task.stdin_content is not None:
                    with NamedTemporaryFile() as tmp: # TODO sorry
                        tmp.write(task.stdin_content)
                        tmp.flush()
                        dup2file(tmp.name, 0, os.O_RDONLY)
                else:
                    dup2devnull(0, os.O_RDONLY)

                write_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC

                if task.stdout is not None:
                    dup2file(task.stdout, 1, write_flags)
                #else:                                      already
                    #dup2devnull(2, os.O_WRONLY)

                if task.stderr is not None:
                    dup2file(task.stderr, 2, write_flags)
                else:
                    dup2devnull(2, os.O_WRONLY)

                if task.setpgrp:
                    os.setpgrp()

                if task.cwd:
                    os.chdir(task.cwd)

                #os.write(2, '++ RUN %r\n' % task.args)
                os.execvp(task.args[0], task.args)

            except BaseException as e:
                try:
                    with os.fdopen(exec_err_wr, 'w') as out:
                        serialize(out, e)
                        out.flush()
                except:
                    pass
                code = 1
            os._exit(code)


    def _read_loop(self):
        channel_in = self._channel_in
        stop_request_received = False

        logging.debug('_Executor._read_loop started')

        while True:
            try:
                msg = deserialize(channel_in)
            except EOFError:
                logging.debug('_Executor._read_loop EOFError')
                break

            if isinstance(msg, NewTaskParamsMessage):
                #logging.debug('_Executor._read_loop NewTaskParamsMessage received')

                if stop_request_received:
                    raise RuntimeError("Message in channel after StopServiceRequestMessage")

                with self._lock:
                    pid, error = self._start_process(msg)
                    self._send_queue.append(ProcessStartMessage(msg.task_id, pid, error))
                    self._send_queue_not_empty.notify()

            elif isinstance(msg, StopServiceRequestMessage):
                #logging.debug('_Executor._read_loop StopServiceRequestMessage received')
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

    def _write_loop(self):
        channel_out = self._channel_out
        send_queue = self._send_queue

        logging.debug('_Executor._write_loop started')

        while True:
            last_iter = False

            with self._lock:
                while not send_queue and not (self._should_stop and not self._running):
                    self._send_queue_not_empty.wait()

                messages = []
                while send_queue:
                    messages.append(send_queue.pop())

                if self._should_stop and not self._running:
                    last_iter = True
                    logging.debug('_Executor._write_loop append(StopServiceResponseMessage)')
                    messages.append(StopServiceResponseMessage())

            for msg in messages:
                serialize(channel_out, msg)
            channel_out.flush()

            if last_iter:
                break

        self._channel.shutdown(socket.SHUT_WR)

    # TODO
        self._write_stopped = True

    def _sig_chld_handler(self, *args):
        with self._lock:
            while self._running:
                pid, status = noeintr(lambda : os.waitpid(-1, os.WNOHANG)) # noeintr not actually need

                if pid == 0:
                    break

                #logging.debug('_Executor._sig_chld_handler pid = %d, status = %d' % (pid, status))

                task_id = self._running.pop(pid)
                self._send_queue.append(ProcessTerminationMessage(task_id, status))

            self._send_queue_not_empty.notify()


    #def run(self):
        #pid_to_task = {}

        #sigchld = PipeEvent()
        #sigchld_fd = sigchld.fd

        #channel_fd = self.channel.fileno()

        #rin = [channel_fd, sigchld_fd]
        #win = [channel_fd]

        #stop_request_received = False

        #send_queue = []


        #while True:
            #rout, wout, _ = select.select(rin, win, [], None) # FIXME EINTR

            #if rout:
                #if sigchld_fd in rout:
                    #sigchld.wait()

                    #while pid_to_task:
                        #pid, status = os.waitpid(-1, os.WNOHANG) # FIXME EINTR
                        #if not pid:
                            #break
                        #task_id = pid_to_task.pop(pid)
                        #send_queue.append(ProcessTerminationMessage(task_id, status))

                #if channel_fd in rout:
                    #msg = _read_message()

                    #if isinstance(msg, NewTaskParamsMessage):
                        #ret_msg = self._start_process(msg)
                        #if ret_msg.pid:
                            #pid_to_task[ret_msg.pid] = msg.task_id
                        #send_queue.append(ret_msg)
                    #elif isinstance(msg, StopServiceRequestMessage):
                        #stop_request_received = True
                    #else:
                        #raise RuntimeError(...)



class _Popen(object):
    def __init__(self, pid, exit_status, send_signal):
        self.pid = pid
        self._exit_status = exit_status
        self._send_signal = send_signal
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

class _Interface(object):
    class Task(object):
        def __init__(self):
            self.pid = Promise()
            self.term_info = Promise()

    def __init__(self, executor_pid, executor_stderr, executor_channel):
        self._executor_pid = executor_pid
        self._executor_stderr = executor_stderr # TODO
        self._channel = executor_channel
        self._channel_out = os.fdopen(self._channel.fileno(), 'w') # FIXME will throw
        self._channel_in  = os.fdopen(self._channel.fileno(), 'r')
        self._input_queue = deque()
        self._lock = threading.Lock()
        self._queue_not_empty = threading.Condition(self._lock)
        self._write_failed = False
        self._should_stop = False
        self._next_task_id = 1
        self._tasks = {}
        self._write_thread = ProfiledThread(target=self._write_loop, name_prefix='RunnerClnWr')
        self._read_thread = ProfiledThread(target=self._read_loop, name_prefix='RunnerClnRd')
        self._write_thread.start()
        self._read_thread.start()

    def stop(self):
        logging.debug('_Interface.stop()')
        with self._lock:
            self._should_stop = True
            self._queue_not_empty.notify()
        self._write_thread.join()
        self._read_thread.join()

    def __del__(self):
        self.stop()
        try:
            self._channel_out.close()
        except:
            pass
        try:
            self._channel_in.close()
        except:
            pass

    def Popen(self, *args, **kwargs):
        with self._lock:
            task_id = self._next_task_id
            self._next_task_id += 1

            msg = NewTaskParamsMessage(*args, **kwargs)
            msg.task_id = task_id

            task = _Interface.Task()
            self._tasks[task_id] = task

            self._input_queue.append(msg)
            self._queue_not_empty.notify()

        pid = task.pid.get_future().get()

        def send_signal(sig):
            return self._send_signal(task_id, sig)

        return _Popen(
            pid=pid,
            exit_status=task.term_info.get_future(),
            send_signal=send_signal
        )

    def _write_loop(self):
        queue = self._input_queue
        channel_out = self._channel_out

        logging.debug('_Interface._write_loop started')

        while True:
            with self._lock:
                while not self._should_stop and not queue:
                    self._queue_not_empty.wait()

                messages = []
                while queue:
                    messages.append(queue.pop())

                if self._should_stop:
                    messages.append(StopServiceRequestMessage())
                    logging.debug('_Interface._write_loop append(StopServiceRequestMessage)')

            for msg in messages:
                serialize(channel_out, msg)
            channel_out.flush()

            if self._should_stop:
                break

        self._channel.shutdown(socket.SHUT_WR)

        logging.debug('_Interface._write_loop finished')

    def _read_loop(self):
        # TODO read stderr of executor to not lock
        channel_in = self._channel_in
        stop_response_received = False

        logging.debug('_Interface._read_loop started')

        while True:
            try:
                msg = deserialize(channel_in)
            except EOFError:
                logging.debug('_Interface._read_loop EOFError')
                break

            if isinstance(msg, ProcessStartMessage):
                #logging.debug('_Interface._read_loop ProcessStartMessage received')

                with self._lock:
                    task = self._tasks[msg.task_id]
                pid, error = msg.pid, msg.error
                if error:
                    error = RuntimeError(error) # FIXME pickle Exception?
                task.pid.set(pid, error)

            elif isinstance(msg, ProcessTerminationMessage):
                #logging.debug('_Interface._read_loop ProcessTerminationMessage received')
                with self._lock:
                    task = self._tasks.pop(msg.task_id)
                    task.term_info.set(msg.exit_status)

            elif isinstance(msg, StopServiceResponseMessage):
                logging.debug('_Interface._read_loop StopServiceResponseMessage received')

                if not self._should_stop: # FIXME more fine check
                    raise RuntimeError('StopServiceResponseMessage received but StopServiceRequestMessage was not send')
                if self._tasks:
                    raise RuntimeError('StopServiceResponseMessage received but not for all process replices was received')

                stop_response_received = True

            else:
                raise RuntimeError('Unknown message type')

        if not stop_response_received:
            raise RuntimeError('Socket closed without StopServiceResponseMessage')

        self._channel.shutdown(socket.SHUT_RD)
        logging.debug('_Interface._read_loop finished')

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
    # XXX TODO Copy-pasted from fork_locking
    dup2devnull(0, os.O_RDONLY)
    dup2devnull(1, os.O_WRONLY)

    #os.dup2(child_err_wr, 2) # TODO
    os.close(child_err_wr)

    ex = _Executor(channel)
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
        logging.debug("_Executor pid = %s" % pid)
        return _Interface(pid, child_err_rd, channel_parent)
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


DEFAULT_RUNNER = None

def CreateDefaultRunner():
    global DEFAULT_RUNNER

    if DEFAULT_RUNNER:
        raise RuntimeError()

    DEFAULT_RUNNER = Runner()

def Popen(*args, **kwargs):
    return DEFAULT_RUNNER.Popen(*args, **kwargs)

def DestroyDefaultRunnerIfNeed():
    global DEFAULT_RUNNER
    if DEFAULT_RUNNER:
        DEFAULT_RUNNER.stop()
        DEFAULT_RUNNER = None

#atexit.register(DestroyDefaultRunnerIfNeed)

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

