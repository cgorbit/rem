import os
import sys
import time
import errno

import threading
from collections import deque
from tempfile import NamedTemporaryFile

from rem.rem_logging import logger as logging
from rem.profile import ProfiledThread
from future import Promise, wrap_future
import rem.delayed_executor as delayed_executor


def sky_share(subproc, directory, files):
    out = NamedTemporaryFile('w')

    argv = ['sky', 'share', '-d', directory] + files
    logging.debug(argv)

    # FIXME collect STDERR
    try:
        p = subproc.Popen(argv, stdout=out.name)
    except:
        try:
            raise
        finally:
            try:
                out.__exit__(None, None, None)
            except:
                pass

    def finalize(exit_code):
        try:
            status = exit_code.get()
            if status:
                raise RuntimeError("sky share failed with exit status: %d" % status)

            with open(out.name) as in_:
                id = in_.readline().rstrip('\n')

            if not id.startswith('rbtorrent:'):
                raise RuntimeError('Malformed output of sky share: %s' % id[:100])

            return id

        finally:
            try:
                out.__exit__(None, None, None)
            except:
                pass

    return wrap_future(p.get_returncode_future(), finalize)


def T(msg):
    pass
    #logging.debug(msg)

class Timing(object):
    def __init__(self, label):
        self.label = label
        self.t0 = None

    def __enter__(self):
        self.t0 = time.time()
        T('before_' + self.label)

    def __exit__(self, t, e, tb):
        T('after_%s = %.3f' % (self.label, time.time() - self.t0))


# XXX TODO For production usage:
# 1. async Sandbox
# 2. vivification-code
# 3. pickable

class Sharer(object):
    def __init__(self, subproc, sandbox, task_owner, task_priority):
        self.subproc = subproc
        self.sandbox = sandbox
        self.task_owner = task_owner
        self.task_priority = task_priority
        self.lock = threading.Lock()
        self.next_job_id = 1
        self.running = {}
        self.jobs_to_retry = {}
        self.share_queue = deque()
        self.share_queue_not_empty = threading.Condition(self.lock)
        self.upload_queue = deque()
        self.upload_queue_not_empty = threading.Condition(self.lock)
        self.wait1_queue = deque()
        self.wait1_queue_not_empty = threading.Condition(self.lock)
        self.wait2_queue = deque()
        self.wait2_queue_not_empty = threading.Condition(self.lock)
        self.should_stop = False

    @classmethod
    def from_context(cls, ctx):
        raise NotImplementedError()

    def UpdateContext(self, ctx):
        raise NotImplementedError()

    def start(self):
        self.threads = [
            ProfiledThread(target=self._share_loop, name_prefix='ResShare'),
            ProfiledThread(target=self._upload_loop, name_prefix='ResUpload'),
            ProfiledThread(target=self._sandbox_wait1_loop, name_prefix='ResWait1'),
            ProfiledThread(target=self._sandbox_wait2_loop, name_prefix='ResWait2'),
        ]

        for t in self.threads:
            t.start()

# TODO This is naive implementation
    def stop(self):
        with self.lock:
            self.should_stop = True

            condvars = [
                self.share_queue_not_empty,
                self.upload_queue_not_empty,
                self.wait1_queue_not_empty,
                self.wait2_queue_not_empty,
            ]

            for condvar in condvars:
                condvar.notify()

        for t in self.threads:
            t.join()

    class Job(object):
        def __init__(self, resource_type, name, directory, files, arch=None, ttl=None,
                           description=None):
            self.id = None
            self.resource_type = resource_type
            self.name = name
            self.directory = directory
            self.files = files
            self.arch = arch
            self.ttl = ttl
            self.description = description
            self.torrent_id = None
            self.upload_task_id = None
            #self.resource_id = None
            self.promise = Promise()

        def __str__(self):
            return 'Job(%d, %s/%s, %s)' % (self.id, self.directory, self.files, self.resource_type)

    class Action(object):
        SHARE_FILE         = 1
        CREATE_UPLOAD_TASK = 2
        WAIT_UPLOAD_TASK   = 3
        FETCH_RESOURCE_ID  = 4

    def _initiate_retry_inner(self, job_id, action):
        if action == self.Action.SHARE_FILE:
            queue, condvar = self.share_queue, self.share_queue_not_empty

        elif action == self.Action.CREATE_UPLOAD_TASK:
            queue, condvar = self.upload_queue, self.upload_queue_not_empty

        elif action == self.Action.CREATE_UPLOAD_TASK:
            queue, condvar = self.wait2_queue, self.wait2_queue_not_empty

        else:
            raise AssertionError("Unreachable. Must not retry for action=%s" % action)

        queue.append(self.running[job_id])
        condvar.notify()

    def _initiate_retry(self, job_id):
        with self.lock:
            action = self.jobs_to_retry.pop(job_id, None)
            if action is None:
                return

            self._initiate_retry_inner(job_id, action)

    def _schedule_retry(self, job, action, delay):
        with self.lock:
            if delay:
                self.jobs_to_retry[job.id] = action

                delayed_executor.schedule(
                    # Unfortunatly delayed_executor has magic about arg count
                    lambda _, id=job.id: self._initiate_retry(id),
                    timeout=delay)
            else:
                self._initiate_retry_inner(job.id, action)

    def share(self, *args, **kwargs):
        job = self.Job(*args, **kwargs)

        with self.lock:
            job_id = self.next_job_id
            self.next_job_id += 1
            job.id = job_id
            self.running[job_id] = job
            self.share_queue.append(job)
            self.share_queue_not_empty.notify()

        logging.debug('New %s' % job)

        #return (job.promise.to_future(), job.id)
        return job.promise.to_future()

    def _set_promise(self, job, val=None, err=None):
        with self.lock:
            self.running.pop(job.id)
        job.promise.set(val, err)

    def _share_loop(self):
        in_progress = set()

        def schedule_retry(job):
            self._schedule_retry(job, self.Action.SHARE_FILE, delay=10.0)

        def _finished(job_id, f):
            T('enter_sky_share_finished %d' % job_id)

            job = self.running[job_id]
            in_progress.remove(job)

            try:
                with Timing('sky_share_future_get %d' % job_id):
                    torrent_id = f.get()
            #except ???Error as e: # TODO
                #pass
            except Exception as e:
                logging.warning('sky share for %s faled: %s' % (job, e))

                # TODO better checks or collect STDERR
                for file in job.files:
                    if not os.path.exists(job.directory + '/' + file):
                        self._set_promise(job, None,
                            OSError(errno.ENOENT,
                                    'Failed to share %s/%s: %s' % (job.directory, job.files, e)))
                        return

                schedule_retry(job)
                return

            logging.debug('sky share successfully done for %s: %s' % (job, torrent_id))

            with self.lock:
                job.torrent_id = torrent_id
                self.upload_queue.append(job)
                self.upload_queue_not_empty.notify()

        while not self.should_stop: # TODO
            T('begin_share_loop')

            with self.lock:
                while not(self.should_stop or self.share_queue):
                    self.share_queue_not_empty.wait()

                if self.should_stop: # TODO
                    return

                job = self.share_queue.popleft()
                in_progress.add(job)

            logging.debug('Run sky share for %s' % job)

            try:
                with Timing('sky_share_future %d' % job.id): # ~4ms (we wait pid from subprocsrv)
                    torrent_id = sky_share(self.subproc, job.directory, job.files)
            except:
                logging.exception('') # TODO
                in_progress.remove(job)
                schedule_retry(job)
                del job
                continue

            torrent_id.subscribe(lambda f, job_id=job.id: _finished(job_id, f))

            del torrent_id
            del job

    def _create_resource_upload_task(self, job):
        return self.sandbox.create_resource_upload_task(
            job.resource_type,
            name=job.name,
            protocol='skynet',
            remote_file_name=job.torrent_id,
            arch=job.arch,
            ttl=job.ttl,
            owner=self.task_owner,
            priority=self.task_priority,
            notifications=[],
        )

    def _try_create_upload_task(self, job):

        # TODO raise on non-recoverabl errors

        try:
            task = self._create_resource_upload_task(job)
        except Exception as e:
            logging.warning('Failed to create upload task %s to Sandbox: %s' % (job, e))
            return

        if job.description:
            try:
                task.update(description=job.description)
            except Exception as e:
                logging.warning('Failed to update task: %s' % (job, e))
                return

        try:
            task.start()
        except Exception as e:
            logging.warning('Failed to start upload task %s to Sandbox: %s' % (job, e))
            return

        return task

    # XXX Single threaded, not production-ready!
    def _upload_loop(self):
        while not self.should_stop:
            T('begin_upload_loop')

            with self.lock:
                while not(self.should_stop or self.upload_queue):
                    self.upload_queue_not_empty.wait()

                if self.should_stop: # TODO
                    return

                job = self.upload_queue.popleft()

            #logging.debug('Uploading to Sandbox %s' % job)

            try:
                with Timing('sbx_create_upload_task for %d' % job.id):
                    task = self._try_create_upload_task(job)
            except Exception as e:
                self._set_promise(job, None, e)
                continue

            if not task:
                self._schedule_retry(job, self.Action.CREATE_UPLOAD_TASK, delay=10.0)
                continue

            job.upload_task_id = task.id
            logging.debug('upload_task_id=%d for %s' % (job.upload_task_id, job))

            with self.lock:
                self.wait1_queue.append(job)
                self.wait1_queue_not_empty.notify()

            del task
            del job

    def _sandbox_wait1_loop(self):
        poll_interval = 3.0
        in_progress = {}

        noop_sandbox_statuses = {
            'DRAFT',
            'ENQUEUING', 'ENQUEUED', 'PREPARING', 'EXECUTING', 'TEMPORARY',
            'FINISHING', 'STOPPING', 'WAIT_RES', 'WAIT_TASK', 'WAIT_TIME',
        }

        next_poll_time = time.time()

        while not self.should_stop:
            T('begin_wait1_loop')

            with self.lock:
                timeout = None
                if in_progress:
                    timeout = max(0.0, next_poll_time - time.time())

                T('before_before_wait1_queue_not_empty_sleep %s' \
                    % ((timeout, self.should_stop, len(self.wait1_queue)),))

                if (timeout is None or timeout) and not(self.should_stop or self.wait1_queue):
                    T('before_wait1_queue_not_empty_sleep %s' % timeout)
                    self.wait1_queue_not_empty.wait(timeout)

                if self.should_stop: # TODO
                    return

                job = None
                if self.wait1_queue:
                    job = self.wait1_queue.popleft()
                    in_progress[job.upload_task_id] = job
                    del job

                if time.time() < next_poll_time:
                    logging.debug('continue_wait1_sleep')
                    continue

            try:
                with Timing('sbx_list_task_statuses'):
                    statuses = self.sandbox.list_task_statuses(in_progress.keys())
            except Exception as e:
                logging.warning("Failed to get sandbox tasks' statuses: %s" % e)
                continue
            finally:
                next_poll_time = max(next_poll_time + poll_interval, time.time())
                T('wait1_next_poll_time=%s' % next_poll_time)

            logging.debug("Task statuses: %s" % statuses) # TODO Comment out

            done = []
            for task_id in in_progress.keys():
                status = statuses.get(task_id)

                if status in noop_sandbox_statuses:
                    continue

                job = in_progress.pop(task_id)

                if status == 'SUCCESS':
                    done.append(job)
                    logging.debug('Upload task=%d in SUCCESS for %s' % (task_id, job))

                elif status in ['FAILURE', 'EXCEPTION'] or status is None:
                    logging.warning("Task %d in FAILURE. Will create new task" % task_id)
                    self._schedule_retry(job, self.Action.CREATE_UPLOAD_TASK, delay=5.0)

                else:
                    logging.error("Unknown task status %s for task=%d, %s" % (status, task_id, job))
                    self._set_promise(job, None,
                        RuntimeError("Unknown task status %s for task_id=%d" % (status, task_id)))

            T('after_process_all_wait1_statuses')

            with self.lock:
                self.wait2_queue.extend(done)
                self.wait2_queue_not_empty.notify()

    def _try_fetch_upload_task_resource_id(self, job):
        task_id = job.upload_task_id

        try:
            ans = self.sandbox.list_task_resources(task_id)
        except Exception as e:
            # XXX TODO WTF
            # 2016-04-21 19:28:15,834 27491 WARNING  resource_sharing:
            # Failed to list resources of task 56706912:
            # HTTPSConnectionPool(host='sandbox.yandex-team.ru', port=443):
            # Max retries exceeded with url: /api/v1.0//resource?task_id=56706912&limit=100
            # (Caused by <class 'httplib.BadStatusLine'>: '')
            logging.warning("Failed to list resources of task %d: %s" % (task_id, e))
            return

        resource_id = None
        for res in ans['items']:
            if res['type'] == job.resource_type:
                resource_id = res['id']

        if not resource_id:
            raise RuntimeError("Resource of type %s not found in task %d resources" \
                % (job.resource_type, task_id))

        return resource_id

    # FIXME Use threads: several _sandbox_wait2_loop's or multithreaded Sandbox
    def _sandbox_wait2_loop(self):
        while not self.should_stop:
            T('begin_wait2_loop')

            with self.lock:
                while not(self.should_stop or self.wait2_queue):
                    with Timing('wait2_queue_wait'):
                        self.wait2_queue_not_empty.wait()

                if self.should_stop: # TODO
                    return

                job = self.wait2_queue.popleft()

            try:
                with Timing('sbx_wait2_fetch_task_resource_id'):
                    resource_id = self._try_fetch_upload_task_resource_id(job)
            except Exception as e:
                logging.warning("Failed to get resource %s from task %d: %s" \
                    % (job.resource_type, job.upload_task_id, e))
                self._set_promise(job, None, e)
                del job
                continue

            if not resource_id:
                self._schedule_retry(job, self.Action.FETCH_RESOURCE_ID, delay=3.0)
            else:
                logging.debug('Done with %s, resource_id = %d' % (job, resource_id))
                self._set_promise(job, resource_id)

            del job

