import threading

class _FutureState(object):
    def __init__(self):
        self._val = None
        self._exc = None
        self._ready_callbacks = []
        self._is_ready = False
        self._lock = threading.Lock()
        self._set_event = threading.Condition(self._lock)

    def set(self, val=None, exc=None):
        to_run = []
        with self._lock:
            if self._is_ready:
                raise RuntimeError("Future state already set")
            self._is_ready = True
            self._val = val
            self._exc = exc
            to_run, self._ready_callbacks = self._ready_callbacks, to_run
            self._set_event.notify_all()

        self._run(to_run)

    def _run(self, to_run):
        future = Future(self) # sorry
        for f in to_run:
            f(future)

    def is_ready(self):
        return self._is_ready

    def subscribe(self, code):
        to_run = []
        with self._lock:
            if self._is_ready:
                to_run.append(code)
            else:
                self._ready_callbacks.append(code)
        self._run(to_run)

    def wait(self, timeout=None):
        if self._is_ready:
            return True

        with self._lock:
            self._set_event.wait(timeout)

        return self._is_ready

    def get_raw(self):
        self.wait()
        return (self._val, self._exc)

    def get(self):
        self.wait()
        if self._exc:
            raise self._exc
        return self._val

class Future(object):
    def __init__(self, state):
        self._state = state

    def is_ready(self):
        return self._state.is_ready()

    def wait(self, timeout=None):
        return self._state.wait(timeout)

    def get(self):
        return self._state.get()

    def get_raw(self):
        return self._state.get_raw()

    def subscribe(self, code):
        self._state.subscribe(code)

class Promise(object):
    def __init__(self):
        self._state = _FutureState()

    def set(self, val=None, exc=None):
        self._state.set(val, exc)

    def get_future(self):
        return Future(self._state)

    def run_and_set(self, code):
        val = None
        exc = None
        try:
            val = code()
        except Exception as exc:
            pass
        self.set(val, exc)

