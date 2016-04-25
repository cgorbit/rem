import time
import threading

from heap import PriorityQueue
from profile import ProfiledThread
from rem_logging import logger as logging

class DelayedExecutor(object):
    def __init__(self):
        self._lock = threading.Lock()
        self._modified = threading.Condition(self._lock)
        self._queue = PriorityQueue()
        self._next_id = 1
        self._should_stop = False
        self._loop_thread = ProfiledThread(target=self._the_loop, name_prefix='DelayedExecutor')
        self._loop_thread.start()

    def _cancel(self, id):
        with self._lock:
            if id not in self._queue: # because of user-space-race with _the_loop
                return False

            if self._queue.front()[0] == id:
                self._modified.notify()

            self._queue.pop_by_key(id)

            return True

    def add(self, callback, deadline=None, timeout=None):
        if timeout is not None:
            deadline = time.time() + timeout
        elif deadline is None:
            raise ValueError("You must specify deadline or timeout")

        with self._lock:
            id = self._next_id
            self._next_id += 1

            self._queue.add(id, (deadline, callback))

            if self._queue.front()[0] == id:
                self._modified.notify()

            return lambda : self._cancel(id) # TODO use weak self

    schedule = add

    def stop(self):
        with self._lock:
            self._should_stop = True
            self._modified.notify()

        self._loop_thread.join()

    def _the_loop(self):
        while not self._should_stop:
            with self._lock:
                while not(self._should_stop or self._queue):
                    self._modified.wait()

                if self._should_stop:
                    return

                id, (deadline, callback) = self._queue.front()

                now = time.time()

                if now < deadline:
                    self._modified.wait(deadline - now)

                    if self._should_stop:
                        return

                    if not self._queue or self._queue.front()[0] != id:
                        continue

                self._queue.pop_front()

            try:
                callback()
            except:
                logging.exception("Failed to execute %s" % callback)

            del callback


_instance = None


def start():
    global _instance
    if _instance:
        raise RuntimeError()
    _instance = DelayedExecutor()


def add(callback, deadline=None, timeout=None):
    global _instance
    _instance.add(callback, deadline, timeout)


def stop():
    global _instance
    if _instance:
        _instance.stop()
        _instance = None
