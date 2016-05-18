#! /usr/bin/env python

import os
import cPickle as pickle
import time
import argparse
import base64
import socket
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, list_public_methods
import xmlrpclib
import rem.xmlrpc
from rem.xmlrpc import is_xmlrpc_exception
from rem.sandbox_remote_packet import WrongTaskIdError

import rem.sandbox_packet
import rem.delayed_executor
from rem.profile import ProfiledThread


def parse_arguments():
    p = argparse.ArgumentParser()

    p.add_argument('--io-dir', dest='io_dir', required=True)
    p.add_argument('--work-dir', dest='work_dir', required=True)
    p.add_argument('--custom-resources', dest='custom_resources')
    p.add_argument('--task-id', dest='task_id', type=int, required=True)
    p.add_argument('--rem-server-addr', dest='rem_server_addr', required=True)
    #p.add_argument('--result-status-file', dest='result_status_file', default='/dev/stdout')
    p.add_argument('--result-snapshot-file', dest='result_snapshot_file')
    p.add_argument('--last-update-message-file', dest='last_update_message_file')

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--snapshot-data', dest='snapshot_data')
    group.add_argument('--snapshot-file', dest='snapshot_file')

    return p.parse_args()


class Context(object):
    def __init__(self):
        self.log_warn_level = 'debug'
        self.log_to_stderr = True

rem.rem_logging.reinit_logger(Context())

from rem.rem_logging import logger as logging

def with_task_id(f):
    def impl(self, task_id, *args):
        self._check_task_id(task_id)

        return f(self, *args)

    impl.__name__ = f.__name__

    return impl


# XXX FIXME Use queue to execute methods?
class RpcMethods(object):
    def __init__(self, pck, task_id):
        self.pck = pck
        self.task_id = task_id

    def _listMethods(self):
        return list_public_methods(self)

    def _check_task_id(self, task_id):
        if task_id != self.task_id:
            raise WrongTaskIdError()

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
    srv = XMLRPCServer(('::', 0), allow_none=True)

    #srv.register_introspection_functions()
    srv.register_instance(RpcMethods(pck, opts.task_id))

    return srv


class RemNotifier(object):
    _RETRY_DELAY = 10.0

    class RetriableError(RuntimeError):
        pass

    def __init__(self, send_update):
        self._send_update = send_update
        self._pending_update = None
        self._pck_finished = False
        self._should_stop_max_time = None
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._worker_thread = ProfiledThread(target=self._loop, name_prefix='RemNotifier')
        self._worker_thread.daemon = True # FIXME See failed[0]
        self._worker_thread.start()

    def stop(self, timeout=0):
        with self._lock:
            if self._should_stop_max_time:
                raise RuntimeError()

            self._should_stop_max_time = time.time() + timeout
            self._changed.notify()

        self._worker_thread.join()

    def send_update(self, update, is_final=False):
        with self._lock:
            self._pending_update = (update, is_final)
            self._changed.notify()

    def _loop(self):
        next_try_min_time = 0

        while True:
            with self._lock:
                while True:
                    now = time.time()

                    if self._should_stop_max_time:
                        if now > self._should_stop_max_time \
                                or next_try_min_time > self._should_stop_max_time:
                            return

                    if self._pending_update:
                        deadline = next_try_min_time

                        if now > deadline:
                            break

                    else:
                        deadline = None

                    self._changed.wait(deadline - now if deadline is not None else None)

                update, is_final = self._pending_update
                self._pending_update = None

            logging.debug('sending_update: %s' % ((update, is_final),))

            try:
                self._send_update(update, is_final)

            except self.RetriableError:
                logging.exception('Failed to send update')

                with self._lock:
                    if not self._pending_update:
                        self._pending_update = (update, is_final)

                    next_try_min_time = time.time() + self._RETRY_DELAY

            else:
                if is_final:
                    return


if __name__ == '__main__':
    opts = parse_arguments()

    for attr in ['io_dir', 'work_dir', 'result_snapshot_file', 'last_update_message_file'] \
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

    notifier_failed = [False]

    def on_notifier_fail():
        notifier_failed[0] = True
        pck.stop(kill_jobs=True) # FIXME Or cancel (do we need snapshot resource)?

    import xmlrpclib
    #rem_proxy = xmlrpclib.ServerProxy('http://' + opts.rem_server_addr,
                                   #allow_none=True,
                                   #)
    rem_proxy = rem.xmlrpc.ServerProxy('http://' + opts.rem_server_addr,
                                   allow_none=True,
                                   timeout=20.0)

    def send_update(update, is_final):
        try:
            return rem_proxy.update_graph(
                opts.task_id,
                rpc_server.server_address[:2],
                update,
                is_final
            )

        except Exception as e:
            logging.exception("on_notifier_fail")

            if is_xmlrpc_exception(e, WrongTaskIdError):
                try:
                    on_notifier_fail()
                except:
                    logging.exception("on_notifier_fail")

                raise RuntimeError("Failed to send data to rem server: %s" % e.faultString)

            else:
                # FIXME Actually if isinstance(e, xmlrpclib.Fault) then not retriable
                #       but not fatal as WrongTaskIdError
                raise RemNotifier.RetriableError(str(e))

    rem_notifier = RemNotifier(send_update)

    #rem_notifier.send_update(pck.produce_rem_update_message()) # FIXME

    rpc_server = _create_rpc_server(pck, opts)

    pck.start(opts.work_dir, opts.io_dir, rem_notifier.send_update)

    ProfiledThread(target=rpc_server.serve_forever, name_prefix='RpcServer').start()

    pck.join()
    logging.debug('after_pck_join')

    last_update_message = pck.produce_rem_update_message()

    if not notifier_failed[0]:
        rem_notifier.send_update(last_update_message, is_final=True)

    rem.delayed_executor.stop() # TODO FIXME Race-condition (can run some in pck)
    rpc_server.shutdown()
    logging.debug('after_rpc_shutdown')

    rem_notifier.stop(30.0) # FIXME
    logging.debug('after_rem_notifier_stop')

    #if not pck.is_cancelled():
    if True:
        with open(opts.result_snapshot_file, 'w') as out:
            pickle.dump(pck, out, 2)

        with open(opts.last_update_message_file, 'w') as out:
            pickle.dump(last_update_message, out, 2)

    logging.debug('after_all')
