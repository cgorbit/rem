import time
import threading

import rem.sandbox as sandbox
from rem.profile import ProfiledThread
from rem_logging import logger as logging


class TaskStateGroups(object):
    DRAFT = 1
    ACTIVE = 2
    TERMINATED = 3


class TaskStatus(object):
    for name in [
        'DRAFT',
        'ENQUEUING', 'ENQUEUED',
        'PREPARING', 'EXECUTING', 'TEMPORARY', 'FINISHING', 'STOPPING', 'SUSPENDING', 'SUSPENDED',
        'WAIT_RES', 'WAIT_TASK', 'WAIT_TIME',
        'SUCCESS', 'RELEASING', 'RELEASED', 'NOT_RELEASED', 'FAILURE', 'DELETING', 'DELETED',
        'NO_RES', 'EXCEPTION', 'TIMEOUT', 'STOPPED',
    ]:
        exec '%s = "%s"' % (name, name)


class SandboxTaskStateAwaiter(object):
    DEFAULT_UPDATE_INTERVAL = 1.0 # FIXME

    def __init__(self, sandbox, update_interval=DEFAULT_UPDATE_INTERVAL):
        self._sandbox = sandbox
        self._should_stop = False
        self._lock = threading.Lock()
        self._something_happend = threading.Condition(self._lock)
        self._worker_thread = None
        self._update_interval = update_interval
        self._running = {}
        self._incoming = set()

    def start(self):
        self._worker_thread = ProfiledThread(target=self._loop, name_prefix='SbxStateMon')
        self._worker_thread.start()

    def stop(self):
        with self._lock:
            self._should_stop = True
            self._something_happend.notify()

        self._worker_thread.join()

    def await(self, task_id):
        with self._lock:
            was_empty = not self._incoming

            assert task_id not in self._incoming and task_id not in self._running

            self._incoming.add(task_id)

            if was_empty:
                self._something_happend.notify()

    def cancel_wait(self, task_id):
        raise NotImplementedError() # TODO

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

                new_jobs, self._incoming = self._incoming, set()

            if new_jobs:
                for task_id in new_jobs:
                    running[task_id] = [None, None]

            self._update()

            next_update_time = time.time() + self._update_interval

    def _update(self):
        running = self._running
        running_task_ids = running.keys()

        # FIXME Determine max &limit for /task in list_task_statuses and split request
        try:
            statuses = self._sandbox.list_task_statuses(running_task_ids)

        except (sandbox.NetworkError, sandbox.ServerInternalError):
            pass

        except Exception:
            logging.exception("Can't fetch task statuses from Sandbox")

        else:
            # FIXME Move to rem.sandbox
            deleted_task_ids = set(running_task_ids) - set(statuses.keys())
            for task_id in deleted_task_ids:
                statuses[task_id] = TaskStatus.DELETED

            for task_id, status in statuses.iteritems():
                task = running[task_id]
                prev_status_group, prev_status = task

                if status != prev_status:
                    logging.debug("task #%d change status %s -> %s" % (task_id, prev_status, status))

                status_group, can_has_res = self._interpret_status(status)

                if status_group == TaskStateGroups.TERMINATED:
                    running.pop(task_id)
                else:
                    task[0] = status_group
                    task[1] = status

                if prev_status_group != status_group:
                    try:
                        self._notify(task_id, status, status_group, can_has_res)
                    except:
                        logging.exception("Sandbox task on_state_change handler failed for %s -> %s" \
                            % (task_id, status))

    def _notify(self, task_id, status_group, can_has_res):
        raise NotImplementedError()

    @staticmethod
    def _interpret_status(status):
        if status == 'DRAFT':
            return (TaskStateGroups.DRAFT, False)

        terminated = {
            # Should have resources
            TaskStatus.SUCCESS:      True,
            TaskStatus.RELEASING:    True, # "impossible"
            TaskStatus.NOT_RELEASED: True, # "impossible"
            TaskStatus.RELEASED:     True, # "impossible"

            # FIXME Resource list can be fetched actually (if it was SUCCESS before DELETED)
            TaskStatus.DELETING:     False,
            TaskStatus.DELETED:      False,

            # FIXME
            #TaskStatus.STOPPED:      False,

            # Can't have resources
            TaskStatus.FAILURE:      False, # "impossible"
            TaskStatus.NO_RES:       False,
            TaskStatus.EXCEPTION:    False,
            TaskStatus.TIMEOUT:      False,
        }

        can_has_res = terminated.get(status)
        if can_has_res is not None:
            return (TaskStateGroups.TERMINATED, can_has_res)

        return (TaskStateGroups.ACTIVE, False)

    #def __getstate__(self):
        #with self._lock:
            #incoming = self._incoming.copy()
            #running  = self._running.copy()

        #incoming = {
            #task_id: SerializableFunction(on_change, [], {})
                #for task_id, on_change in incoming.iteritems()
        #}

        #running = {
            #task_id: [prev_status_group, SerializableFunction(on_change, [], {}), prev_status]
                #for task_id, (prev_status_group, on_change, prev_status) in running
        #}
