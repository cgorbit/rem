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

    def get(self, timeout=None):
        if not self.wait(timeout):
            raise NotReadyError()
        if self._exc:
            if isinstance(self._exc, tuple):
                raise self._exc[0], self._exc[1], self._exc[2]
            else:
                raise self._exc
        return self._val

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

    def __nonzero__(self):
        return self.is_set()

    def wait(self, timeout=None):
        return self._state.wait(timeout)

    def get(self, timeout=None):
        return self._state.get(timeout)

    def get_raw(self, timeout=None):
        return self._state.get_raw(timeout)

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

class _FuturesAwaiter(object):
    _MAX_EXCEPTIONS = 10

    def __init__(self, futures):
        self._future_count = len(futures)
        self._exceptions = []
        self._promise = Promise()
        self._lock = threading.Lock()
        for f in futures:
            f.subscribe(self._on_set)

    def _on_set(self, f):
        with self._lock:
            assert self._future_count

            self._future_count -= 1

            #if self._promise.is_set():
                #return

            #elif not f.is_success():
                #self._promise.set(None, f.get_raw()[1])

            if not f.is_success():
                exc = f.get_raw()[1]
                if isinstance(exc, tuple):
                    exc = exc[0]
                self._exceptions.append(exc)

            if not self._future_count:
                val = None
                exc = None

                if self._exceptions:
                    exc = RuntimeError("%d futures failed: %s" % (
                        len(self._exceptions),
                        '\n'.join(map(repr, self._exceptions[:self._MAX_EXCEPTIONS]))))
                self._exceptions = None

                self._promise.set(val, exc)

    def to_future(self):
        return self._promise.to_future()

def WaitFutures(futures):
    return _FuturesAwaiter(futures).to_future()
