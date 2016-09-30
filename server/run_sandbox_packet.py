#! /usr/bin/env python

import os
import cPickle as pickle
import time
import argparse
import base64
import socket
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, list_public_methods
import rem.xmlrpc
from rem.xmlrpc import is_xmlrpc_exception
from rem.sandbox_remote_packet import WrongTaskIdError
import errno
import json

import rem.common
from rem.common import parse_network_address, logged
import rem.sandbox_packet
import rem.delayed_executor
from rem.profile import ProfiledThread
from rem.job_graph import GraphState


class Context(object):
    def __init__(self):
        self.log_warn_level = 'debug'
        self.log_to_stderr = True

rem.rem_logging.reinit_logger(Context())

from rem.rem_logging import logger as logging


def parse_arguments():
    p = argparse.ArgumentParser()

    p.add_argument('--io-dir', dest='io_dir', required=True)
    p.add_argument('--work-dir', dest='work_dir', required=True)
    p.add_argument('--task-id', dest='task_id', type=int, required=True)
    p.add_argument('--rem-server-addr', dest='rem_server_addr', required=True)
    p.add_argument('--result-snapshot-file', dest='result_snapshot_file')
    p.add_argument('--last-update-message-file', dest='last_update_message_file')
    p.add_argument('--last-update-user-summary-file', dest='last_update_user_summary_file')
    p.add_argument('--listen-port', dest='listen_port')
    p.add_argument('--resume-params', dest='resume_params')

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--snapshot-data', dest='snapshot_data')
    group.add_argument('--snapshot-file', dest='snapshot_file')

    return p.parse_args()


def with_task_id(f):
    def impl(self, task_id, *args):
        self._check_task_id(task_id)

        return f(self, *args)

    impl.__name__ = f.__name__

    return impl


def logged_rpc(f):
    return logged(log_args=True, skip_arg_count=1)(f)


# XXX FIXME Use queue to execute methods async?
class RpcMethods(object):
    def __init__(self, pck, task_id):
        self.pck = pck
        self.task_id = task_id

    def _listMethods(self):
        return list_public_methods(self)

    def _check_task_id(self, task_id):
        if task_id != self.task_id:
            raise WrongTaskIdError("Sandbox task_id == %s, but got request for %s" % (self.task_id, task_id))

    @logged_rpc
    @with_task_id
    def restart(self):
        self.pck.restart()

    @logged_rpc
    @with_task_id
    def resume(self):
        self.pck.resume()

    @logged_rpc
    @with_task_id
    def stop(self, kill_jobs):
        self.pck.stop(kill_jobs)

    @logged_rpc
    @with_task_id
    def cancel(self):
        self.pck.cancel()

    @logged_rpc
    @with_task_id
    def ping(self):
        pass

    @logged_rpc
    @with_task_id
    def list_all_user_processes(self):
        return rem.common.list_all_user_processes()


class XMLRPCServer(SimpleXMLRPCServer):
    address_family = socket.AF_INET6 # + hope that IPV6_V6ONLY is off in /sys

    def __repr__(self):
        addr, port = self.server_address[:2]
        return '<XMLRPCServer addr=%s, port=%s at 0x%x>' % (addr, port, id(self))


def _do_create_rpc_server(listen_port):
    def _create(port):
        return XMLRPCServer(('::', port), allow_none=True)

    if listen_port is None:
        return _create(0)

    elif isinstance(listen_port, int):
        return _create(listen_port)

    else:
        # TODO use random
        for port in xrange(listen_port[0], listen_port[1] + 1):
            try:
                return _create(port)
            except socket.error as e:
                if e.errno != errno.EADDRINUSE:
                    raise
        raise socket.error(errno.EADDRINUSE, "Can't listen on any addr in range")

def _create_rpc_server(pck, opts):
    srv = _do_create_rpc_server(opts.listen_port)

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
        self._worker_thread = ProfiledThread(target=self._the_loop, name_prefix='RemNotifier')
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

    def _the_loop(self):
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


class OnExit(object):
    def __init__(self, code):
        self._code = code

    def __enter__(self):
        pass

    def __exit__(self, t, e, tb):
        self._code()


class OnDel(object):
    def __init__(self, code):
        self._code = code

    def __del__(self):
        self._code()


def _parse_listen_port(opt):
    def _check_port(port):
        if not(port > 0 and port < 65536):
            raise ValueError("Bad port number")

    if '-' in opt:
        min, max = map(int, opt.split('-'))
        _check_port(min)
        _check_port(max)
        if max < min:
            raise ValueError("Incorrect port range")
        return (min, max)
    else:
        port = int(opt)
        _check_port(port)
        return port


def _absolutize_fs_options(opts):
    names = [
        'io_dir',
        'work_dir',
        'result_snapshot_file',
        'last_update_message_file',
        'snapshot_file',
        'last_update_user_summary_file',
    ]

    for name in names:
        value = getattr(opts, name, None)
        if value is not None:
            setattr(opts, name, os.path.abspath(value))


def guess_my_host(peer_addr, timeout):
    try:
        s = socket.create_connection(peer_addr, timeout=timeout)
    except socket.error as e:
        logging.debug('Can\'t connect to %s: %s' % (peer_addr, e))
        return

    return s.getsockname()[0]


def try_guess_my_host(peer_addr, timeout):
    try:
        return guess_my_host(peer_addr, timeout)
    except:
        logging.exception('Failed to guess_my_host')


def try_log_descriptors():
    try:
        import subprocess
        files = subprocess.check_output(['lsof', '-p', str(os.getpid())])
    except:
        logging.exception('Failed to dump lsof')
    else:
        logging.debug('lsof\n' + files)


if __name__ == '__main__':
    opts = parse_arguments()

    _absolutize_fs_options(opts)

# TODO XXX Pass as it should be
    opts.listen_port = '15000-15999'

    if opts.listen_port is not None:
        try:
            opts.listen_port = _parse_listen_port(opts.listen_port)
        except:
            logging.exception("Failed to parse --listen-port")

    os.chdir(opts.work_dir)

# TODO delayed_executor guard
# TODO rpc_server guard
# TODO rem_notifier guard

    rem.delayed_executor.start()
    #guard = OnDel(rem.delayed_executor.stop)

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

    rem_proxy = rem.xmlrpc.ServerProxy('http://' + opts.rem_server_addr,
                                   allow_none=True,
                                   timeout=20.0)

    def send_update(update, is_final):
        try:
            return rem_proxy.update_graph(
                opts.task_id,
                rpc_server_addr,
                update,
                is_final
            )

        except Exception as e:
            if isinstance(e, socket.error):
                logging.warning("on_notifier_fail: %s" % e)
            else:
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

# TODO _create_rpc_server may throw errno.EADDRINUSE
    rpc_server = _create_rpc_server(pck, opts)

    try_log_descriptors()
    logging.debug('rpc_server.server_address = %s' % (rpc_server.server_address,))

    my_host = try_guess_my_host(parse_network_address(opts.rem_server_addr), timeout=3.0)
    logging.debug('guessed host = %s' % my_host)

    rpc_server_addr = (
        my_host or os.uname()[1],
        rpc_server.server_address[1]
    )

    reset_tries = False

    if opts.resume_params:
        resume_params = json.loads(opts.resume_params)

        if resume_params.get('use_dummy_jobs', False):
            import rem.job
            rem.job.DUMMY_COMMAND_CREATOR = lambda job: ['true'] # TODO sleep

        reset_tries = resume_params.get('reset_tries', False)

    pck.start(
        opts.work_dir,
        opts.io_dir,
        rem_notifier.send_update,
        reset_tries=reset_tries,
    )

    ProfiledThread(target=rpc_server.serve_forever, name_prefix='RpcServer').start()

    pck.join()
    logging.debug('after_pck_join')

    last_update_message = pck.produce_rem_update_message()

    if not notifier_failed[0]:
        rem_notifier.send_update(last_update_message, is_final=True)

    rem.delayed_executor.stop()
    rpc_server.shutdown()
    logging.debug('after_rpc_shutdown')

    rem_notifier.stop(1.0) # FIXME XXX TODO :)
    logging.debug('after_rem_notifier_stop')

    #if not pck.is_cancelled():
    if True:
        with open(opts.result_snapshot_file, 'w') as out:
            pickle.dump(pck, out, 2)

        with open(opts.last_update_message_file, 'w') as out:
            pickle.dump(last_update_message, out, 2)


        if opts.last_update_user_summary_file:
            with open(opts.last_update_user_summary_file, 'w') as out:
                print >>out, json.dumps({'status': GraphState.str(pck.state)}, indent=3)

    logging.debug('after_all')
