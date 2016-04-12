import sys
import threading


class NotReadyError(RuntimeError):
    pass


class _FutureState(object):
    def __init__(self):
        self._val = None
        self._exc = None
        self._ready_callbacks = []
        self._is_set = False
        self._lock = threading.Lock()
        self._set_event = threading.Condition(self._lock)

    def set(self, val=None, exc=None):
        to_run = []

        with self._lock:
            if self._is_set:
                raise RuntimeError("Future state already set")
            self._is_set = True
            self._val = val
            self._exc = exc
            to_run, self._ready_callbacks = self._ready_callbacks, to_run
            self._set_event.notify_all()

        self._run(to_run)

    def _run(self, to_run):
        future = Future(self) # sorry
        for f in to_run:
            f(future)

    def _repr_inter(self):
        is_set = self._is_set

        ret = '%sset' % (
            '' if is_set else 'not '
        )

        if is_set:
            ret += ' to '
            exc = self._exc
            if exc is not None:
                if isinstance(exc, tuple):
                    exc = exc[1]
                ret += type(exc).__name__ + ' exception'
            else:
                ret += type(self._val).__name__ + ' type'

        return ret

    def is_set(self):
        return self._is_set

    def subscribe(self, code):
        to_run = []
        with self._lock:
            if self._is_set:
                to_run.append(code)
            else:
                self._ready_callbacks.append(code)
        if to_run:
            self._run(to_run)

    def wait(self, timeout=None):
        if self._is_set:
            return True

        with self._lock:
            if not self._is_set:
                self._set_event.wait(timeout)

        return self._is_set

    def get_raw(self, timeout=None):
        if not self.wait(timeout):
            raise NotReadyError()
        return (self._val, self._exc)

    def get_exception(self, timeout=None):
        return self.get_raw(timeout)[1]

    def get(self, timeout=None):
        if not self.wait(timeout):
            raise NotReadyError()
        if self._exc:
            if isinstance(self._exc, tuple):
                raise self._exc[0], self._exc[1], self._exc[2]
            else:
                raise self._exc
        return self._val

    def get_nonblock(self, default=None):
        if self._is_set:
            return self.get()
        return default

    def is_success(self):
        if not self._is_set:
            raise NotReadyError()
        return not self._exc


def _repr(self):
    return '<%s.%s %s at 0x%x>' % (
        self.__module__,
        type(self).__name__,
        self._state._repr_inter(),
        id(self)
    )


class Future(object):
    def __init__(self, state):
        self._state = state

    def is_set(self):
        return self._state.is_set()

    def wait(self, timeout=None):
        return self._state.wait(timeout)

    def get(self, timeout=None):
        return self._state.get(timeout)

    def get_raw(self, timeout=None):
        return self._state.get_raw(timeout)

    def get_exception(self, timeout=None):
        return self._state.get_exception(timeout)

    def get_nonblock(self, default=None):
        return self._state.get_nonblock(default)

    def subscribe(self, code):
        self._state.subscribe(code)

    def is_success(self):
        return self._state.is_success()

    def __repr__(self):
        return _repr(self)

class Promise(object):
    def __init__(self):
        self._state = _FutureState()

    def set(self, val=None, exc=None):
        self._state.set(val, exc)
        return self

    def is_set(self):
        return self._state.is_set()

    def to_future(self):
        return Future(self._state)

    def run_and_set(self, code):
        val = None
        exc = None
        try:
            val = code()
        except Exception:
            exc = sys.exc_info()
        self.set(val, exc)

    def raise_and_set(self, exc):
        def raise_():
            raise exc
        self.run_and_set(raise_)

    def set_from_current_exception(self):
        exc = sys.exc_info()
        if exc[0] is None:
            raise RuntimeError()
        self.set(exc=exc)

    def __repr__(self):
        return _repr(self)


READY_FUTURE = Promise().set(None).to_future()


class FutureCombinerBase(object):
    def __init__(self, futures):
        self.__pending_count = len(futures)
        self._promise = Promise()
        self.__lock = threading.Lock()

        for idx, f in enumerate(futures):
            f.subscribe(lambda f, idx=idx: self.__on_set(f, idx))

    def __on_set(self, f, idx):
        with self.__lock:
            assert self.__pending_count
            self.__pending_count -= 1

            self._on_set(f, idx)

            if not self.__pending_count:
                self._on_after_all()

    def to_future(self):
        return self._promise.to_future()

    def _on_set(self, f, idx):
        pass

    def _on_after_all(self):
        pass


class _FutureResultsJoiner(FutureCombinerBase):
    def __init__(self, futures):
        self.__results = [None for idx in xrange(len(futures))]
        self.__failed = False
        FutureCombinerBase.__init__(self, futures)

    def _on_set(self, f, idx):
        if f.is_success():
            self.__results[idx] = f.get()
        else:
            self.__failed = True
            self._promise.set(None, f.get_exception())

    def _on_after_all(self):
        if self.__failed:
            return

        self._promise.set(self.__results)


def JoinFuturesResults(futures):
    return _FutureResultsJoiner(futures).to_future()


class _AllFuturesAwaiter(FutureCombinerBase):
    def __init__(self, futures):
        FutureCombinerBase.__init__(self, futures)

    def _on_after_all(self):
        self._promise.set(None)


def WaitAllFutures(futures):
    return _AllFuturesAwaiter(futures).to_future()


class _AllFutureSucceedChecker(FutureCombinerBase):
    def __init__(self, futures):
        self.__failed = False
        FutureCombinerBase.__init__(self, futures)

    def _on_set(self, f, idx):
        if not f.is_success():
            self.__failed = True
            self._promise.set(None, f.get_exception())

    def _on_after_all(self):
        if not self.__failed:
            self._promise.set(None)


def CheckAllFuturesSucceed(futures):
    return _AllFutureSucceedChecker(futures).to_future()

