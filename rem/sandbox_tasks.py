import time
import threading

import rem.sandbox as sandbox
from rem.profile import ProfiledThread
from rem_logging import logger as logging


class TaskStateGroups(object):
    DRAFT = 1
    ACTIVE = 2
    TERMINATED = 3
    ANY = 4


class SandboxTaskStateAwaiter(object):
    DEFAULT_UPDATE_INTERVAL = 5.0

    def __init__(self, sandbox, update_interval=DEFAULT_UPDATE_INTERVAL):
        self._sandbox = sandbox
        self._should_stop = False
        self._lock = threading.Lock()
        self._something_happend = threading.Condition(self._lock)
        self._worker_thread = None
        self._update_interval = update_interval
        self._running  = {}
        self._incoming = {}

        self._start()

    def _start(self):
        self._worker_thread = ProfiledThread(target=self._loop, name_prefix='SbxStateMon')
        self._worker_thread.start()

    def stop(self):
        with self._lock:
            self._should_stop = True
            self._something_happend.notify()

        self._main_thread.join()

    def await(self, task_id, on_change):
        with self._lock:
            was_empty = not self._incoming

            assert task_id not in self._incoming and task_id not in self._running

            self._incoming[task_id] = on_change

            if was_empty:
                self._something_happend.notify()

    def _loop(self):
        running = self._running
        next_update_time = time.time() + self._update_interval

        while True:
            with self._lock:
                while True:
                    if self._should_stop:
                        return

                    if not self._incoming and not running:
                        deadline = None
                    else:
                        now = time.time()
                        if now > next_update_time:
                            break
                        deadline = next_update_time - now

                    self._something_happend.wait(deadline)

                new_jobs, self._incoming = self._incoming, {}

            if new_jobs:
                for task_id, on_change in new_jobs.items():
                    running[task_id] = [None, on_change]

            self._update()

            next_update_time = time.time() + self._update_interval

    def _update(self):
        running = self._running

        try:
            statuses = self._sandbox.list_task_statuses(running.keys())

        except (sandbox.NetworkError, sandbox.ServerInternalError) as e:
            pass

        except Exception as e:
            logging.exception("Can't fetch task statuses from Sandbox")

        else:
            for task_id, status in statuses.iteritems():
                prev_status_group, on_change = running[task_id]

                status_group = self._to_status_group(status)

                if status_group == TaskStateGroups.TERMINATED:
                    running.pop(task_id)

                if prev_status_group != status_group:
                    try:
                        on_change(task_id, status_group)
                    except:
                        logging.exception("Sandbox task on_state_change handler failed for %s -> %s" \
                            % (task_id, status))

    @staticmethod
    def _to_status_group(status):
        map = {
            'DRAFT': TaskStateGroups.DRAFT,

            'SUCCESS':   TaskStateGroups.TERMINATED,
            'RELEASING': TaskStateGroups.TERMINATED,
            'RELEASED':  TaskStateGroups.TERMINATED,
            'FAILURE':   TaskStateGroups.TERMINATED,
            'DELETING':  TaskStateGroups.TERMINATED,
            'DELETED':   TaskStateGroups.TERMINATED,
            'NO_RES':    TaskStateGroups.TERMINATED,
            'EXCEPTION': TaskStateGroups.TERMINATED,
            'TIMEOUT':   TaskStateGroups.TERMINATED,
        }

        return map.get(status, TaskStateGroups.ACTIVE)

    #def _in_status_group(status, group):
        #if group == TaskStateGroups.ANY:
            #return True

        #if status == 'DRAFT':
            #return group == TaskStateGroups.DRAFT

        #elif status in ['SUCCESS', 'RELEASING', 'RELEASED', 'FAILURE', 'DELETING',
                        #'DELETED', 'NO_RES', 'EXCEPTION', 'TIMEOUT']:
            #return group == TaskStateGroups.TERMINATED

        #else:
            #return group == TaskStateGroups.ACTIVE


