from __future__ import with_statement
import logging
import os
import time
import datetime

from callbacks import CallbackHolder
from common import FuncRunner, SendEmail, Unpickable, safeint, nullobject, zeroint
import osspec
import packet
import constants
from rem.profile import ProfiledThread
import process_proxy
import pgrpguard # TODO remove

DUMMY_COMMAND_CREATOR = None

def cut_message(msg, BEG_LEN=None, FIN_LEN=None):
    BEG_LEN = BEG_LEN or 1000
    FIN_LEN = FIN_LEN or 1000
    if msg and len(msg) > BEG_LEN + FIN_LEN + 5:
        msg = msg[:BEG_LEN] + "\n...\n" + msg[-FIN_LEN:]
    return msg or ""

class IResult(Unpickable(type=str,
                         code=safeint,
                         message=str)):
    def __init__(self, type, code, message):
        self.type = type
        self.code = code
        self.message = message

    def IsSuccessfull(self):
        return self.code == 0

    def IsFailed(self):
        return (self.code is not None) and (self.code != 0)

    def CanRetry(self):
        return True

    def __str__(self):
        return "%s: %s" % (self.type, self.code) + (", \"%s\"" % self.message if self.message else "")


class CommandLineResult(IResult):
    time_format = "%Y/%m/%d %H:%M:%S"

    def __init__(self, code, start_time, fin_time, err, max_err_len=None):
        IResult.__init__(self, "OS exit code", code, "started: %s; finished: %s;%s" \
                                                     % (
            time.strftime(self.time_format, start_time), time.strftime(self.time_format, fin_time),
            "\n" + cut_message(err, max_err_len / 2 if max_err_len else None,
                               max_err_len / 2 if max_err_len else None) if err else ""))


class JobStartErrorResult(CommandLineResult):
    def __init__(self, jobId, exception_message):
        ts = datetime.datetime.fromtimestamp(time.time())
        IResult.__init__(self, "Job start error", 1, "Job %s start error at %s, error message: %s" % (jobId, ts.strftime(self.time_format), exception_message))


class TriesExceededResult(IResult):
    def __init__(self, maxcount):
        IResult.__init__(self, "The number of attempts exceeded", maxcount, None)

    def CanRetry(self):
        return False


class TimeOutExceededResult(IResult):
    time_format = CommandLineResult.time_format

    def __init__(self, jobId):
        ts = datetime.datetime.fromtimestamp(time.time())
        IResult.__init__(self, "Timeout exceeded", 1, "Job %s timelimit exceeded at %s" % (jobId, ts.strftime(self.time_format)))


class PackedExecuteResult(IResult):
    def __init__(self, doneCount, allCount):
        if doneCount != allCount:
            IResult.__init__(self, "Unsuccessfull completion of packet work", 1, "%s/%s done" % (doneCount, allCount))
        else:
            IResult.__init__(self, "Successfull completion of packet work", 0, "%s/%s done" % (doneCount, allCount))


class Job(Unpickable(err=nullobject,
                     results=list,
                     tries=int,
                     pipe_fail=bool,
                     description=str,
                     max_working_time=(int, constants.KILL_JOB_DEFAULT_TIMEOUT),
                     notify_timeout=(int, constants.NOTIFICATION_TIMEOUT),
                     working_time=int,
                     _notified=bool,
                     output_to_status=bool),
          CallbackHolder):
    ERR_PENALTY_FACTOR = 6

    def __init__(self, shell, parents, pipe_parents, packetRef, maxTryCount, limitter, max_err_len=None,
                 retry_delay=None, pipe_fail=False, description="", notify_timeout=constants.NOTIFICATION_TIMEOUT, max_working_time=constants.KILL_JOB_DEFAULT_TIMEOUT, output_to_status=False):
        super(Job, self).__init__()
        self.maxTryCount = maxTryCount
        self.limitter = limitter
        self.shell = shell
        self.parents = parents
        self.inputs = pipe_parents
        self.id = id(self)
        self.max_err_len = max_err_len
        self.retry_delay = retry_delay
        self.pipe_fail = pipe_fail
        self.description = description
        self.notify_timeout = notify_timeout
        self.max_working_time = max_working_time
        if self.limitter:
            self.AddCallbackListener(self.limitter)
        self.packetRef = packetRef
        self.AddCallbackListener(self.packetRef)
        self.output_to_status = output_to_status
        self._process = None
        self._should_stop = False

    @staticmethod
    def __read_stream(fh, buffer):
        buffer.append(fh.read())

    def __wait_process(self, process, err_pipe):
        out = []
        stderrReadThread = ProfiledThread(
            target=Job.__read_stream,
            args=(err_pipe, out),
            name_prefix='StdErr',
        )
        stderrReadThread.setDaemon(True)
        stderrReadThread.start()
        if process.stdin:
            process.stdin.close()
        last_update_time = time.time()
        working_time = 0
        poll = process.poll
        _time = time.time
        while poll() is None:
            working_time += _time() - last_update_time
            last_update_time = _time()
            if working_time < self.notify_timeout < self.max_working_time and not self._notified:
                if process.wait(self.notify_timeout - working_time) is None:
                    self._timeoutNotify(working_time + _time() - last_update_time)

            elif working_time < self.max_working_time:
                if process.wait(self.max_working_time - working_time) is None:
                    process.terminate()
                    self.results.append(TimeOutExceededResult(self.id))
                    break
            time.sleep(0.001)
        stderrReadThread.join()
        return "", out[0]

    def _timeoutNotify(self, working_time):
        self.cached_working_time = working_time
        msgHelper = packet.PacketCustomLogic(self.packetRef).DoLongExecutionWarning(self)
        SendEmail(self.packetRef.notify_emails, msgHelper)
        self._notified = True

    def CanStart(self):
        if self.limitter:
            return self.limitter.CanStart()
        return True

    def _finalize_job_iteration(self, result):
        try:
            if result is not None:
                self.results.append(result)
            if (result is None) \
                or (result.IsFailed() and self.tries >= self.maxTryCount \
                    and self.packetRef.state in (packet.PacketState.WORKABLE, packet.PacketState.PENDING)):
                if result is not None:
                    logging.info("Job`s %s result: TriesExceededResult", self.id)
                    self.results.append(TriesExceededResult(self.tries))
                if self.packetRef.kill_all_jobs_on_error:
                    self.packetRef.Suspend(kill_jobs=True)
                    self.packetRef.changeState(packet.PacketState.ERROR)
        except:
            logging.exception("some error during job finalization")
            if self.packetRef.kill_all_jobs_on_error:
                self.packetRef.Suspend(kill_jobs=True)
                self.packetRef.changeState(packet.PacketState.ERROR)

    def _make_run_args(self):
        return [osspec.get_shell_location()] \
            + (["-o", "pipefail"] if self.pipe_fail else []) \
            + ["-c", self.shell]

    @staticmethod
    def start_process(*args, **kwargs):
        wrapper = packet.PacketCustomLogic.SchedCtx.process_wrapper

        if wrapper is None:
            return process_proxy.ProcessProxy(*args, **kwargs)
        else:
            kwargs['wrapper_binary'] = wrapper
            return process_proxy.ProcessGroupGuardProxy(*args, **kwargs)

    def Run(self):
        self._should_stop = False
        self.input = self.output = None
        self.errPipe = None
        jobResult = None
        proc = None
        try:
            self.tries += 1
            self.working_time = 0
            self.FireEvent("start")
            startTime = time.localtime()
            self.errPipe = map(os.fdopen, os.pipe(), 'rw')

            run_args = DUMMY_COMMAND_CREATOR(self) if DUMMY_COMMAND_CREATOR \
                       else self._make_run_args()

            logging.debug("out: %s, in: %s", self.output, self.input)

            self._process = proc = self.start_process(
                run_args,
                stdout=self.output.fileno(),
                stdin=self.input.fileno(),
                stderr=self.errPipe[1].fileno(),
                close_fds=True,
                cwd=self.packetRef.directory,
            )

            self.errPipe[1].close()

            if self._should_stop:
                proc.terminate()

            _, err = self.__wait_process(proc, self.errPipe[0])
            retCode = proc.wait()

            jobResult = CommandLineResult(retCode, startTime, time.localtime(), err,
                                       getattr(self, "max_err_len", None))
        except Exception as e:
            if not proc or isinstance(e, pgrpguard.ProcessStartError): # TODO don't use pgrpguard here
                jobResult = JobStartErrorResult(None, str(e))
            logging.exception("Run job %s exception: %s", self.id, e)

        finally:
            self._process = None
            self._finalize_job_iteration(jobResult)
            self.CloseStreams()
            self.FireEvent("done")

    def CloseStreams(self):
        if self.output_to_status and self.output:
            try:
                if not self.output.closed:
                    self.output.close()
                if self.output.closed:
                    try:
                        self.output = open(self.output.name, 'r')
                    except IOError:
                        #packet probably was deleted and place released
                        self.output = open('/dev/null', 'r')

                if len(self.results):
                    self.results[-1].message = self.results[-1].message or ""
                    self.results[-1].message += '\nOutput:\n'
                    self.results[-1].message += '-'*80
                    self.results[-1].message += '\n'+'\n'.join(self.output.readlines())
            except:
                logging.exception("can't save jobs output")

        closingStreamGenerators = (
            lambda: self.input,
            lambda: self.output,
            lambda: self.errPipe[0] if self.errPipe else None,
            lambda: self.errPipe[1] if self.errPipe else None
        )

        for fn in closingStreamGenerators:
            stream = fn()
            if stream is not None:
                try:
                    if isinstance(stream, file):
                        if not stream.closed:
                            stream.close()
                    elif isinstance(stream, int):
                        os.close(stream)
                    else:
                        raise RuntimeError("can't close unknown file object %r" % stream)
                except:
                    logging.exception('close stream error')

    def Terminate(self):
        self._should_stop = True
        proc = self._process
        if proc:
            proc.terminate()

    def Result(self):
        return self.results[-1] if self.results else None


class FuncJob(object):
    def __init__(self, runner):
        assert isinstance(runner, FuncRunner), "incorrent arguments for FuncJob initializing"
        self.runner = runner

    def Run(self, pids=None):
        self.runner()
