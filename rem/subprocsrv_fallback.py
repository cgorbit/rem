import os

import subprocess
import subprocsrv


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

    def __enter__(self):
        return self._file.__enter__()

    def __exit__(self, *args):
        self._file.__exit__(*args)


class _Popen(subprocess.Popen):
    def __init__(*args, **kwargs):
        stdin_content = kwargs.pop('stdin_content', None)

        task = subprocsrv.NewTaskParamsMessage(*args, **kwargs)

        if task.use_pgrpguard:
            raise ValueError("use_pgrpguard is not supported")

        def file_or_none(filename, mode):
            return file_or_scoped_default(filename, mode, None)

        stdin_mgr = _NamedTemporaryFileWithContent(stdin_content) \
            if stdin_content is not None \
            else file_or_none(task.stdin, 'r')

        with stdin_mgr as stdin:
            with file_or_none(task.stdout, 'w') as stdout:
                with file_or_none(task.stderr, 'w') as stderr:
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
                    #logging.error(repr(refl))

                    # XXX at least shell option meaning differs (in list argv)
                    subprocess.Popen.__init__(self, **refl)

    def send_signal_safe(self, sig, group=False):
        raise NotImplementedError()

    def wait_no_throw(self, timeout=None, deadline=None):
        return self.wait(timeout, deadline)

    def wait(self, timeout=None, deadline=None):
        raise NotImplementedError() # TODO

    def is_terminated(self):
        self.poll()
        return self.returncode is not None

    def communicate(self):
        raise NotImplementedError()

    def pipe_cloexec(self):
        raise NotImplementedError()


def Runner(object):
    @classmethod
    def start(cls, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def Popen(cls, *args, **kwargs):
        return _Popen(*args, **kwargs)

    @classmethod
    def check_call(cls, *args, **kwargs):
        return _check_call(cls.call, args, kwargs)

    @classmethod
    def call(cls, *args, **kwargs):
        return cls._Popen(*args, **kwargs).wait()

    @classmethod
    def stop(cls):
        pass


# TODO Move
def _check_call(call, args, kwargs):
    retcode = call(*args, **kwargs)
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = args[0]
        check_retcode(retcode, cmd)
    return 0
