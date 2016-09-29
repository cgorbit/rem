import os
import subprocess
from tempfile import NamedTemporaryFile

import subprocsrv
from common import check_process_call, check_process_retcode, wait as _wait_process
from osspec import set_oom_adj


class ScopedVal(object):
    def __init__(self, val):
        self._val = val

    def __enter__(self):
        return self._val

    def __exit__(self, *args):
        pass


def file_or_scoped_default(file, mode, default):
    if file is None:
        return ScopedVal(default)
    elif isinstance(file, str):
        return open(file, mode)
    else:
        raise ValueError()


class _NamedTemporaryFileWithContent(object):
    def __init__(self, content):
        self._file = NamedTemporaryFile()
        self._file.write(content)
        self._file.flush()
        self._file.file.seek(0)

    def __enter__(self):
        return self._file.__enter__()

    def __exit__(self, *args):
        self._file.__exit__(*args)


class _Popen(object):
    def __init__(self, *args, **kwargs):
        self.returncode = None

        stdin_content = kwargs.pop('stdin_content', None)

        task = subprocsrv.NewTaskParamsMessage(*args, **kwargs)

        if task.use_pgrpguard:
            raise ValueError("use_pgrpguard is not supported")

        def file_or_none(filename, mode):
            return file_or_scoped_default(filename, mode, None)

        stdin_mgr = _NamedTemporaryFileWithContent(stdin_content) \
            if stdin_content is not None \
            else file_or_none(task.stdin, 'r')

        env = None
        if task.env_update:
            env = os.environ.copy()
            env.update(task.env_update)

        def preexec_fn():
            if task.setpgrp:
                os.setpgrp()
            if task.oom_adj is not None:
                set_oom_adj(task.oom_adj)

        with stdin_mgr as stdin:
            with file_or_none(task.stdout, 'w') as stdout:
                with file_or_none(task.stderr, 'w') as stderr:
                    refl=dict(
                        args=task.args,
                        stdin=stdin,
                        stdout=stdout,
                        stderr=stderr,
                        preexec_fn=preexec_fn,
                        cwd=task.cwd,
                        shell=task.shell,
                        close_fds=True, # as in _Server
                        env=env,
                    )
                    #logging.error(repr(refl))

                    # XXX at least shell option meaning differs (in list argv)
                    self._impl = subprocess.Popen(**refl)

    def send_signal_safe(self, sig, group=False):
        raise NotImplementedError()

    def send_signal(self, sig, group=False):
        raise NotImplementedError()

    def wait_no_throw(self, timeout=None, deadline=None):
        if self.returncode:
            return self.returncode

        _wait_process(self._impl.poll, timeout, deadline)

        self.returncode = self._impl.returncode
        return self.returncode

    wait = wait_no_throw

    def poll(self):
        return self.wait(timeout=0)

    def is_terminated(self):
        return self.returncode is not None


class Runner(object):
    @staticmethod
    def start(*args, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def Popen(*args, **kwargs):
        return _Popen(*args, **kwargs)

    @classmethod
    def check_call(cls, *args, **kwargs):
        return check_process_call(cls.call, args, kwargs)

    @staticmethod
    def call(*args, **kwargs):
        return _Popen(*args, **kwargs).wait()

    @classmethod
    def stop(cls):
        pass


class RunnerWithFallback(object):
    def __init__(self, main, fallback):
        self._main = main
        self._fallback = fallback
        self._broken = False

    def Popen(self, *args, **kwargs):
        if self._broken:
            return self._fallback.Popen(*args, **kwargs)

        try:
            return self._main.Popen(*args, **kwargs)

        except subprocsrv.ServiceUnavailable:
            self._broken = True
            return self._fallback.Popen(*args, **kwargs)

    def check_call(self, *args, **kwargs):
        return check_process_call(self.call, args, kwargs)

    def call(self, *args, **kwargs):
        return self.Popen(*args, **kwargs).wait()

    def stop(self):
        pass


