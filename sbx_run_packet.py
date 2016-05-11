#! /usr/bin/env python

import os
import cPickle as pickle
import argparse
import base64
import socket
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, list_public_methods
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


class RpcMethods(object):
    def __init__(self, pck, task_id):
        self.pck = pck
        self.task_id = task_id

    def _listMethods(self):
        return list_public_methods(self)

    def _check_task_id(self, task_id): # TODO Use decorator
        if task_id != self.task_id:
            raise WrongTaskId()

    def rpc_restart(self, task_id):
        self._check_task_id(task_id)
        self.pck.restart()

    def rpc_stop(self, task_id, kill_jobs):
        self._check_task_id(task_id)
        self.pck.stop(kill_jobs)

    def rpc_cancel(self, task_id):
        self._check_task_id(task_id)
        self.pck.cancel()

    def rpc_ping(self, task_id):
        self._check_task_id(task_id)
        return self.task_id


class XMLRPCServer(SimpleXMLRPCServer):
    address_family = socket.AF_INET6 # + hope that IPV6_V6ONLY is off in /sys


def _create_rpc_server(pck, opts):
    srv = XMLRPCServer(('::', 0))

    #srv.register_introspection_functions()
    srv.register_task(RpcMethods(pck, opts.task_id))

    threading.Thread(target=srv.serve_forever).start()

    return srv


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

    def on_update(state_history, detailed_status):
        pass # TODO

    pck.start(opts.work_dir, opts.io_dir, on_update)

    rpc_server = _create_rpc_server(pck, opts)

    pck.join()
    rpc_server.shutdown()
    rem.delayed_executor.stop() # TODO FIXME Race-condition (can run some in pck)

# TODO

    with open(opts.result_file, 'w') as out:
        pass # TODO
