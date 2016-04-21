import os
import sys
import time

import threading
from collections import deque
from tempfile import NamedTemporaryFile

from rem.rem_logging import logger as logging
from rem.profile import ProfiledThread
from future import Promise, wrap_future

def sky_share(subproc, filename):
    with NamedTemporaryFile('w') as out:
        p = subproc.Popen(['sky', 'share', filename], stdout=out.name)

        if p.wait():
            raise RuntimeError("sky share failed with exit status: %d" % p.returncode)

        with open(out.name) as in_:
            id = in_.readline().rstrip('\n')

    if not id.startswith('rbtorrent:'):
        raise RuntimeError('Malformed output of sky share: %s' % id[:100])

    return id


def sky_share_future(subproc, filename):
    out = NamedTemporaryFile('w')

    try:
        p = subproc.Popen(['sky', 'share', filename], stdout=out.name)
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


class Sharer(object):
    def __init__(self, subproc, sandbox, task_owner, task_priority, default_ttl=None):
        self.subproc = subproc
        self.sandbox = sandbox
        self.task_owner = task_owner
        self.task_priority = task_priority
        self.default_ttl = default_ttl
        self.lock = threading.Lock()
        self.next_job_id = 1
        self.running = {}
        self.share_queue = deque()
        self.share_queue_not_empty = threading.Condition(self.lock)
        self.upload_queue = deque()
        self.upload_queue_not_empty = threading.Condition(self.lock)
        self.wait_queue = deque()
        self.wait_queue_not_empty = threading.Condition(self.lock)
        self.should_stop = False

        self.share_thread  = ProfiledThread(target=self._share_loop, name_prefix='ResShare')
        self.upload_thread = ProfiledThread(target=self._upload_loop, name_prefix='ResUpload')
        self.sandbox_wait_thread   = ProfiledThread(target=self._sandbox_wait_loop, name_prefix='ResWait')

        self.share_thread.start()
        self.upload_thread.start()
        self.sandbox_wait_thread.start()

    def stop(self):
        raise NotImplementedError()

    class Job(object):
        def __init__(self, resource_type, name, filename, arch=None, ttl=None):
            self.id = None
            self.resource_type = resource_type
            self.name = name
            self.filename = filename
            self.arch = arch
            self.ttl = ttl
            self.torrent_id = None
            self.upload_task_id = None
            #self.resource_id = None
            self.promise = Promise()

        def __str__(self):
            return 'Job(%d, %s, %s)' % (self.id, self.filename, self.resource_type)

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

        return job.promise.to_future()

    def _share_loop(self):
        in_progress = [0]

        def _finished(job_id, f):
            job = self.running[job_id]

            try:
                torrent_id = f.get()
            except:
                logging.exception('sky share for %s faled' % job)
                raise # NotImplementedError #2
            else:
                logging.debug('sky share sucessfully done for %s' % job)

            with self.lock:
                in_progress[0] -= 1
                job.torrent_id = torrent_id
                self.upload_queue.append(job)
                self.upload_queue_not_empty.notify()

        while not self.should_stop: # TODO
            with self.lock:
                while not(self.should_stop or self.share_queue):
                    self.share_queue_not_empty.wait()

                if self.should_stop: # TODO
                    return

                job = self.share_queue.popleft()
                in_progress[0] += 1

            logging.debug('Run sky share for %s' % job)

            try:
                torrent_id = sky_share_future(self.subproc, job.filename)
            except:
                logging.exception('sky share for %s faled' % job)
                raise # NotImplementedError #1

            try:
                torrent_id.subscribe(lambda f, job_id=job.id: _finished(job_id, f))
            except:
                logging.exception('sky share for %s faled' % job)
                raise # NotImplementedError #2

            del torrent_id
            del job

    def _create_resource_upload_task(self, job):
        return self.sandbox.create_resource_upload_task(
            job.resource_type,
            name=job.name,
            protocol='skynet',
            remote_file_name=job.torrent_id,
            arch=job.arch,
            ttl=job.ttl or self.default_ttl,
            owner=self.task_owner,
            priority=self.task_priority,
            notifications=[],
        )

    # FIXME Run several _upload_loop workers?
    def _upload_loop(self):
        while not self.should_stop:
            with self.lock:
                while not(self.should_stop or self.upload_queue):
                    self.upload_queue_not_empty.wait()

                if self.should_stop: # TODO
                    return

                job = self.upload_queue.popleft()

            logging.debug('Uploading to Sandbox %s' % job)

            try:
                sbx_task = self._create_resource_upload_task(job)
            except Exception as e:
                logging.exception('Failed to create upload task %s to Sandbox' % job)
                raise # NotImplementedError #3

            try:
                sbx_task.start()
            except Exception as e:
                logging.exception('Failed to start upload task %s to Sandbox' % job)
                raise # NotImplementedError #3

            # TODO Check sbx_task

            job.upload_task_id = sbx_task.id
            logging.debug('upload_task_id=%d for %s' % (job.upload_task_id, job))

            with self.lock:
                self.wait_queue.append(job)
                self.wait_queue_not_empty.notify()

            del sbx_task
            del job

    def _sandbox_wait_loop(self):
        poll_interval = 3.0
        in_progress = {}

        noop_sandbox_statuses = {
            'DRAFT',
            'ENQUEUING', 'ENQUEUED', 'PREPARING', 'EXECUTING', 'TEMPORARY',
            'FINISHING', 'STOPPING', ' WAIT_RES', 'WAIT_TASK', 'WAIT_TIME',
        }

        next_poll_time = 0.0

        while not self.should_stop:
            with self.lock:
                timeout = None
                if in_progress:
                    timeout = max(0.0, time.time() - next_poll_time)

                if (timeout is None or timeout) and not(self.should_stop or self.wait_queue):
                    self.wait_queue_not_empty.wait(timeout)

                if self.should_stop: # TODO
                    return

                job = None
                if self.wait_queue:
                    job = self.wait_queue.popleft()
                    in_progress[job.upload_task_id] = job
                    del job

                if time.time() < next_poll_time:
                    continue

            try:
                statuses = self.sandbox.list_task_statuses(in_progress.keys()) # FIXME Better?
            except:
                logging.exception("Failed to get sandbox tasks' statuses")
                continue

            logging.debug("Task statuses: %s" % statuses)

            done = []
            for task_id, status in statuses.iteritems():
                if status == 'SUCCESS':
                    job = in_progress.pop(task_id)
                    done.append(job)
                    logging.debug('Upload task=%d in SUCCESS for %s' % (task_id, job))
                elif status in noop_sandbox_statuses:
                    pass
                elif status == 'FAILURE':
                    raise NotImplementedError("task %d status %s" % (task_id, status))
                else: # TODO
                    raise NotImplementedError("task %d status %s" % (task_id, status))

            # FIXME Use threads (for example _lookup_ids_loop) or multithreaded Sandbox
            for job in done:
                try:
                    task = self.sandbox.Task(job.upload_task_id)
                except:
                    raise # NotImplementedError() #5

                try:
                    resource_id = task.release['resources'][0]['resource_id']
                except:
                    raise # NotImplementedError() #6

                job.promise.set(resource_id)

            with self.lock:
                for job in done:
                    self.running.pop(job.id)

            next_poll_time = max(next_poll_time + poll_interval, time.time())
