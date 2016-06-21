from Queue import Queue as ThreadSafeQueue

from rem.profile import ProfiledThread
from rem_logging import logger as logging

class ActionQueue(object):
    __STOP_INDICATOR = object()

    def __init__(self, thread_count=1, thread_name_prefix='Thread'):
        self._queue = ThreadSafeQueue()
        self._thread_name_prefix = thread_name_prefix
        self._thread_count = thread_count
        self.__start()

    def __start(self):
        self._threads = [
            ProfiledThread(target=self.__worker, name_prefix=self._thread_name_prefix)
                for _ in xrange(self._thread_count)
        ]

        for t in self._threads:
            t.start()

    def stop(self):
        for _ in self._threads:
            self._queue.put(self.__STOP_INDICATOR)

        for t in self._threads:
            t.join()

    def invoke(self, task):
        self._queue.put(task)

    put = invoke

    def __worker(self):
        while True:
            task = self._queue.get()

            if task is self.__STOP_INDICATOR:
                break

            try:
                task()
            except Exception:
                logging.exception("")

