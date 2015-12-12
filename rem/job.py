from __future__ import with_statement
import logging
import os
import time
import datetime
import threading

from callbacks import CallbackHolder
from rem.common import FuncRunner, SendEmail, Unpickable, safeint, nullobject, zeroint
import osspec
import packet
import constants
from rem.profile import ProfiledThread
import runner

DUMMY_COMMAND_CREATOR = None


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

    def __init__(self, code, start_time, fin_time, err):
        IResult.__init__(self, "OS exit code", code, "started: %s; finished: %s;%s" \
                                                     % (
            time.strftime(self.time_format, start_time), time.strftime(self.time_format, fin_time),
            "\n" + (err or "")))


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
                     output_to_status=bool,
                     alive=bool,
                     running_pids=set),
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

    #@staticmethod
    #def __read_stream(fh, buffer):
        #buffer.append(fh.read())

    def __wait_process(self, process):
        #out = []
        #stderrReadThread = ProfiledThread(name_prefix='JobStderr', target=Job.__read_stream, args=(err_pipe, out))
        #stderrReadThread = threading.Thread(target=Job.__read_stream, args=(err_pipe, out))
        #stderrReadThread.setDaemon(True)
        #stderrReadThread.start()

        start_time = time.time() # FIXME set earlier

        #if process.stdin:
            #process.stdin.close()

        code = None

        if self.notify_timeout < self.max_working_time: # FIXME bullshit?
            code = process.wait(deadline=start_time + self.notify_timeout)

            if code is None:
                self._timeoutNotify(time.time() - start_time)
            else:
                return code

        code = process.wait(deadline=start_time + self.max_working_time)

        if code is None:
            process.kill() # TODO
            self.results.append(TimeOutExceededResult(self.id)) # TODO

        #stderrReadThread.join()
        #return "", out[0]

        return process.wait()

    def _timeoutNotify(self, working_time):
        self.cached_working_time = working_time
        msgHelper = packet.PacketCustomLogic(self.packetRef).DoLongExecutionWarning(self)
        SendEmail(self.packetRef.notify_emails, msgHelper)
        self._notified = True

    def CanStart(self):
        if self.limitter:
            return self.limitter.CanStart()
        return True

    def _finalize_job_iteration(self, pid, result, pidTrackers):
        try:
            self.alive = False
            if pidTrackers and pid is not None:
                for tracker in pidTrackers:
                    tracker.discard(pid)
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

    def Run(self, worker_trace_pids=None):
        self.alive = True
        self.input = None
        self.output = None
        self.error_output = None
        #self.errPipe = None
        jobResult = None
        jobPid = None
        pidTrackers = [self.running_pids] + ([] if worker_trace_pids is None else [worker_trace_pids])
        try:
            self.tries += 1
            self.working_time = 0
            self.FireEvent("start")
            startTime = time.localtime()
            #self.errPipe = map(os.fdopen, os.pipe(), 'rw')

            run_args = DUMMY_COMMAND_CREATOR(self) if DUMMY_COMMAND_CREATOR \
                       else self._make_run_args()

            logging.debug("out: %s, in: %s", self.output, self.input)
            #process = subprocess.Popen(run_args, stdout=self.output.fileno(), stdin=self.input.fileno(),
                                       #stderr=self.errPipe[1].fileno(), close_fds=True, cwd=self.packetRef.directory,
                                       #preexec_fn=os.setpgrp)
            process = runner.Popen(
                run_args,
                stdout=self.output.name,
                stdin=self.input.name,
                #stderr=self.errPipe[1].fileno(),
                stderr=self.error_output.name,
                cwd=self.packetRef.directory,
                setpgrp=True
            )

            jobPid = process.pid

            for tracker in pidTrackers:
                tracker.add(jobPid)

            #self.errPipe[1].close()

            #in case when we stopped during start implicitly kill himself
            if not self.alive:
                self.Terminate()

            #_, err = self.__wait_process(process, self.errPipe[0])
            retCode = self.__wait_process(process)

            err = self._produce_legacy_stderr_output(
                self.error_output.name, self.max_err_len or 2000)

            jobResult = CommandLineResult(retCode, startTime, time.localtime(), err)

        except Exception, e:
            if not jobPid:
                jobResult = JobStartErrorResult(None, str(e))
            logging.exception("Run job %s exception: %s", self.id, e)

        finally:
            self._finalize_job_iteration(jobPid, jobResult, pidTrackers)
            self.CloseStreams()
            self.FireEvent("done")

    @staticmethod
    def _produce_legacy_stderr_output(filename, max_err_len):
        full_size = os.stat(filename).st_size

        with open(filename) as input:
            if full_size > max_err_len + 5:
                msg = input.read(max_err_len / 2)
                msg += '\n...\n'
                input.seek(-(max_err_len / 2), os.SEEK_END)
                msg += input.read()
            else:
                msg = input.read()

        return msg

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
            lambda: self.error_output,
            #lambda: self.errPipe[0] if self.errPipe else None,
            #lambda: self.errPipe[1] if self.errPipe else None
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
        self.alive = False
        pids = self.running_pids
        if pids:
            logging.debug("trying to terminate processes with pids: %s", ",".join(map(str, pids)))
            for pid in list(pids):
                osspec.terminate(pid)

    def Result(self):
        return self.results[-1] if self.results else None


class FuncJob(object):
    def __init__(self, runner):
        assert isinstance(runner, FuncRunner), "incorrent arguments for FuncJob initializing"
        self.runner = runner

    def Run(self, pids=None):
        self.runner()
