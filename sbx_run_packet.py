#! /usr/bin/env python

import os
import cPickle as pickle
import argparse
import base64
import socket
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, list_public_methods
import rem.xmlrpc
import rem.rem_logging

import rem.sandbox_packet
import rem.delayed_executor


def parse_arguments():
    p = argparse.ArgumentParser()

    p.add_argument('--io-dir', dest='io_dir', required=True)
    p.add_argument('--work-dir', dest='work_dir', required=True)
    p.add_argument('--custom-resources', dest='custom_resources')
    p.add_argument('--task-id', dest='task_id', required=True)
    p.add_argument('--rem-server-addr', dest='rem_server_addr', required=True)
    p.add_argument('--result-file', dest='result_file', default='/dev/stdout')

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--snapshot-data', dest='snapshot_data')
    group.add_argument('--snapshot-file', dest='snapshot_file')

    return p.parse_args()


class Context(object):
    def __init__(self):
        self.log_warn_level = 'debug'
        self.log_to_stderr = True

rem.rem_logging.reinit_logger(Context())


def with_task_id(f):
    def impl(self, task_id, *args):
        self._check_task_id(task_id)

        return f(self, *args)

    impl.__name__ = f.__name__

    return impl


class WrongTaskId(RuntimeError):
    pass


class RpcMethods(object):
    def __init__(self, pck, task_id):
        self.pck = pck
        self.task_id = task_id

    def _listMethods(self):
        return list_public_methods(self)

    def _check_task_id(self, task_id):
        if task_id != self.task_id:
            raise WrongTaskId()

    @with_task_id
    def rpc_restart(self):
        self.pck.restart()

    @with_task_id
    def rpc_stop(self, kill_jobs):
        self.pck.stop(kill_jobs)

    @with_task_id
    def rpc_cancel(self):
        self.pck.cancel()

    @with_task_id
    def rpc_ping(self):
        pass


class XMLRPCServer(SimpleXMLRPCServer):
    address_family = socket.AF_INET6 # + hope that IPV6_V6ONLY is off in /sys


def _create_rpc_server(pck, opts):
    srv = XMLRPCServer(('::', 0))

    #srv.register_introspection_functions()
    srv.register_task(RpcMethods(pck, opts.task_id))

    threading.Thread(target=srv.serve_forever).start()

    return srv


class RemNotifier(object):
    def __init__(self, addr):
        self._proxy = rem.xmlrpc.ServerProxy('http://' + addr)
        self._pending_update = None
        self._pck_finished = False
        self._should_stop_max_time = None
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._worker_thread = ProfiledThread(target=self._loop, name_prefix='RemNotifier')
        self._worker_thread.start()

    def stop(self, timeout=0):
        with self._lock:
            if self._should_stop_max_time:
                raise RuntimeError()

            self._should_stop_max_time = time.time() + timeout
            self._changed.notify()

        self._worker_thread.join()

    def send_update(self, state):
        with self._lock:
            self._pending_update = state
            self._changed.notify()

    def notify_finished(self):
        with self._lock:
            self._pck_finished = True
            self._changed.notify()

    def _loop(self):
        next_try_min_time = 0

        while True:
            with self._lock:
                while True:
                    now = time.time()

                    if self._should_stop_max_time:
                        if now > self._should_stop_max_time \
                            or self._pending_update and self._pck_finished \
                                and next_try_min_time > self._should_stop_max_time:
                        return

                    if self._pending_update or self._pck_finished:
                        deadline = next_try_min_time

                        if now > deadline:
                            break

                    else:
                        deadline = None

                    self._changed.wait(deadline - now)

                update, self._pending_update = self._pending_update, None

            if update:
raise NotImplementedError() # TODO task_id
                try:
                    self._proxy.up...date(update)
                except:
                    with self._lock:
                        if not self._pending_update:
                            self._pending_update = update

                        next_try_min_time = time.time() + self._retry_delay
                        continue

            with self._lock:
                if not(self._pck_finished and not self._pending_update):
                    continue

raise NotImplementedError() # TODO task_id
            try:
                self._proxy.set_finished()
            except:
                next_try_min_time = time.time() + self._retry_delay
                continue


if __name__ == '__main__':
    opts = parse_arguments()

    for attr in ['io_dir', 'work_dir', 'result_file'] \
            + (['snapshot_file'] if opts.snapshot_file is not None else []):
        setattr(opts, attr, os.path.abspath(getattr(opts, attr)))

    os.chdir(opts.work_dir)

    rem.delayed_executor.start()

    if opts.snapshot_file is not None:
        with open(opts.snapshot_file) as snap:
            pck = pickle.load(snap)
    else:
        pck = pickle.loads(base64.b64decode(opts.snapshot_data))

    pck.vivify_jobs_waiting_stoppers()

    rem_notifier = RemNotifier(opts.rem_server_addr)

    rem_notifier.send_update(pck.produce_rem_update_message())

    pck.start(opts.work_dir, opts.io_dir, rem_notifier.send_update)

    rpc_server = _create_rpc_server(pck, opts)

    pck.join()
    rem_notifier.notify_finished()

    rem.delayed_executor.stop() # TODO FIXME Race-condition (can run some in pck)
    rpc_server.shutdown()
    rem_notifier.stop(30.0)

    with open(opts.result_file, 'w') as out:
        pickle.dump(pck.produce_rem_update_message(), out, 2)
