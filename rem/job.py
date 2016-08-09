# -*- coding: utf-8 -*-
from __future__ import with_statement
import os
import time
import datetime
import sys

from common import SerializableFunction, Unpickable, safeint
import osspec
import constants
import job_process
import pgrpguard
from rem_logging import logger as logging
import subprocsrv

DUMMY_COMMAND_CREATOR = None
_DEFAULT_STDERR_SUMMAY_LEN = 2000


# XXX #1
def _RunnerWithFallbackFunctor(self, main, fallback):
    broken = [False]

    def impl(*args, **kwargs):
        if broken[0]:
            return fallback(*args, **kwargs)

        try:
            return main(*args, **kwargs)

        except subprocsrv.ServiceUnavailable:
            broken[0] = True
            return fallback(*args, **kwargs)

    return impl

# XXX #2
class _RunnerWithFallbackFunctor(object):
    def __init__(self, main, fallback):
        self._main = main
        self._fallback = fallback
        self._broken = False

    def __call__(self, *args, **kwargs):
        if self._broken:
            return self._fallback(*args, **kwargs)

        try:
            return self._main(*args, **kwargs)

        except subprocsrv.ServiceUnavailable:
            self._broken = True
            return self._fallback(*args, **kwargs)


def create_job_runner(runner, pgrpguard_binary):
    def subprocsrv_backend(*args, **kwargs):
        if pgrpguard_binary is not None:
            kwargs['use_pgrpguard'] = True
        return job_process.SubprocsrvProcess(runner, *args, **kwargs)

    def ordinal_backend(*args, **kwargs):
        kwargs['close_fds'] = True

        if pgrpguard_binary is None:
            return job_process.DefaultProcess(*args, **kwargs)

        else:
            kwargs['wrapper_binary'] = pgrpguard_binary
            return job_process.PgrpguardProcess(*args, **kwargs)

    return _RunnerWithFallbackFunctor(subprocsrv_backend, ordinal_backend) \
        if runner \
        else ordinal_backend


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

    def __init__(self, code, start_time, finish_time, err):
        def format_time(t):
            if t is None:
                return None
            return time.strftime(self.time_format, time.localtime(t))

        IResult.__init__(self, "OS exit code", code, "started: %s; finished: %s;%s" \
            % (format_time(start_time), format_time(finish_time), "\n" + (err or "")))


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


class PackedExecuteResult(object): # for old backups
    pass


class Job(Unpickable(results=list,
                     tries=int,
                     pipe_fail=bool,
                     description=str,
                     max_working_time=(int, constants.KILL_JOB_DEFAULT_TIMEOUT),
                     notify_timeout=(int, constants.NOTIFICATION_TIMEOUT),
                     cached_working_time=int,
                     output_to_status=bool)):
    ERR_PENALTY_FACTOR = 6

    def __init__(self, pck_id, shell, parents, pipe_parents, max_try_count, max_err_len=None,
                 retry_delay=None, pipe_fail=False, description="", notify_timeout=constants.NOTIFICATION_TIMEOUT, max_working_time=constants.KILL_JOB_DEFAULT_TIMEOUT, output_to_status=False):
        super(Job, self).__init__()
        self.id = id(self)
        self.pck_id = pck_id
        self.max_try_count = max_try_count
        self.shell = shell
        self.parents = parents
        self.inputs = pipe_parents
        self.max_err_len = max_err_len
        self.retry_delay = retry_delay
        self.pipe_fail = pipe_fail
        self.description = description
        self.notify_timeout = notify_timeout
        self.max_working_time = max_working_time
        self.output_to_status = output_to_status

    def __repr__(self):
        return "<Job(id: %s; packet: %s)>" % (self.id, self.pck_id)

    def full_id(self):
        return "%s.%s" % (self.pck_id, self.id)

    def __getstate__(self):
        return self.__dict__.copy()

    def __make_run_args(self):
        return [osspec.get_shell_location()] \
            + (["-o", "pipefail"] if self.pipe_fail else []) \
            + ["-c", self.shell]

    def make_run_args(self):
        return DUMMY_COMMAND_CREATOR(self) if DUMMY_COMMAND_CREATOR \
                    else self.__make_run_args()

    def last_result(self):
        return self.results[-1] if self.results else None

    def produce_legacy_stderr_output(self, filename):
        return self._produce_legacy_stderr_output(filename, self.max_err_len or _DEFAULT_STDERR_SUMMAY_LEN)

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

    @staticmethod
    def _produce_stdout_status(filename):
        with open(filename) as stream:
            ret  = ""
            ret += '\nOutput:\n'
            ret += '-' * 80
            ret += '\n' + stream.read()
        return ret


class JobRunner(object):
    def __init__(self, ops, job):
        self._ops = ops
        self._job = job
        self._cancelled = False

        self._process = None
        self._start_time = None
        self._finish_time = None
        self._timedout = False

        self._environment_error = None
        self._process_start_error = None

        self._stderr_summary = None
        self._stdout_summary = None

    def _wait_process(self, process):
        job = self._job
        start_time = time.time() # FIXME set earlier

        code = None

        if job.notify_timeout < job.max_working_time: # FIXME bullshit?
            code = process.wait(deadline=start_time + job.notify_timeout)

            if code is None:
                job.cached_working_time = time.time() - start_time
                self._ops.notify_long_execution(job)
            else:
                return code

        code = process.wait(deadline=start_time + job.max_working_time)

        if code is None:
            process.terminate()
            self._timedout = process.was_signal_sent() # there's race with self._cancelled

        return process.wait()

    def _run_process_inner(self, argv, stdin, stdout, stderr):
        job = self._job
        ops = self._ops
        self._start_time = time.time()

        try:
            process = ops.start_process(
                argv,
                stdout=stdout,
                stdin=stdin,
                stderr=stderr,
                cwd=ops.get_working_directory(),
                env_update=[('REM_PACKET_ID', job.pck_id)],
            )

        except pgrpguard.Error as e:
            t, e, tb = sys.exc_info()
            raise t, e, tb

        except Exception as e:
            self._process_start_error = e
            logging.exception("Failed to start %s: %s" % (job, e))
            return False

        self._process = process

        if self._cancelled:
            process.terminate()

        try:
            self._wait_process(process)
        except Exception as e:
            t, e, tb = sys.exc_info()
            try:
                process.terminate()
            except:
                pass
            raise t, e, tb

        self._finish_time = time.time()

        return True

    def _run_process(self, *args):
        try:
            return self._run_process_inner(*args)

        except (pgrpguard.WrapperNotFoundError, pgrpguard.WrapperStartError) as e:
            self._environment_error = e

        except pgrpguard.ProcessStartOSError as e:
            self._process_start_error = e

        return False

    def _run(self):
        job = self._job

        argv = job.make_run_args()

        try:
            stdin, stdout, stderr = self._ops.create_file_handles(job)
        except Exception as e:
            self._environment_error = e
            return

        with stdin:
            with stdout:
                with stderr:
                    logging.debug("out: %s, in: %s", stdout, stdin)
                    logging.debug("job %s\tstarted", job.shell)

                    if not self._run_process(argv, stdin, stdout, stderr):
                        return

                    self._stderr_summary = job.produce_legacy_stderr_output(stderr.name)

                    if job.output_to_status:
                        self._stdout_summary = job._produce_stdout_status(stdout.name)

    def _append_results(self):
        job = self._job

        def append_result(result):
            job.results.append(result)

        if self._timedout:
            append_result(TimeOutExceededResult(job.id))

        as_legacy_start_error = self._process_start_error or self._environment_error

        if as_legacy_start_error:
            append_result(JobStartErrorResult(job.id, str(as_legacy_start_error)))

        elif self._finish_time:
            append_result(
                CommandLineResult(self._process.returncode,
                                    self._start_time,
                                    self._finish_time,
                                    self._stderr_summary)
            )

        if self.returncode_robust != 0 and job.tries >= job.max_try_count:
            logging.info("Job's %s result: TriesExceededResult", job.id)
            append_result(TriesExceededResult(job.tries))

        if job.output_to_status and job.results and self._stdout_summary:
            last = job.results[-1]
            last.message = last.message or ""
            last.message += self._stdout_summary

    def run(self):
        job = self._job

        try:
            try:
                self._run()
            except Exception:
                # FIXME Do something more than logging?
                logging.exception("Run job %s exception", job.id)

            if not(self._environment_error or self._cancelled):
                job.tries += 1

            self._append_results()

        finally:
            self._ops.on_job_done(self)

    def cancel(self):
        self._cancelled = True
        proc = self._process
        if proc:
            proc.terminate()

    def is_cancelled(self):
        return self._cancelled

    @property
    # Will not throw
    def returncode_robust(self):
        if not self._finish_time:
            return None
        return self._process.returncode

    @property
    def job(self):
        return self._job

    def __repr__(self):
        return '<JobRunner for %s>' % repr(self._job)


class FuncJob(object):
    def __init__(self, runner):
        assert isinstance(runner, SerializableFunction), "incorrent arguments for FuncJob initializing"
        self.runner = runner

    def run(self):
        self.runner()

    def cancel(self):
        logging.warning("Can't Terminate FuncJob %s" % self.runner)
