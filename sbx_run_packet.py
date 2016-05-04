#! /usr/bin/env python

import os
import cPickle as pickle
import argparse
import base64
import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer, list_public_methods

import rem.sandbox_packet
import rem.delayed_executor


def parse_arguments():
    p = argparse.ArgumentParser()

    p.add_argument('--io-dir', dest='io_dir', required=True)
    p.add_argument('--work-dir', dest='work_dir', required=True)
    p.add_argument('--custom-resources', dest='custom_resources')
    p.add_argument('--instance-id', dest='instance_id', required=True)
    p.add_argument('--rem-server-addr', dest='rem_server_addr', required=True)
    p.add_argument('--result-file', dest='result_file', default='/dev/stdout')

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--snapshot-data', dest='snapshot_data')
    group.add_argument('--snapshot-file', dest='snapshot_file')

    return p.parse_args()


class RpcMethods(object):
    def __init__(self, pck, instance_id):
        self.pck = pck
        self.instance_id = instance_id

    def _listMethods(self):
        return list_public_methods(self)

    def rpc_restart(self):
        self.pck.restart()

    def rpc_stop(self, kill_jobs):
        self.pck.stop(kill_jobs)

    def rpc_cancel(self):
        self.pck.cancel()

    def rpc_ping(self):
        return self.instance_id


class XMLRPCServer(SimpleXMLRPCServer):
    address_family = socket.AF_INET6 # + hope that IPV6_V6ONLY is off in /sys


def _create_rpc_server(pck, opts):
    srv = XMLRPCServer()

    #srv.register_introspection_functions()
    srv.register_instance(RpcMethods(pck, opts.instance_id))

    threading.Thread(target=srv.serve_forever).start()

    return srv

if __name__ == '__main__':
    opts = parse_arguments()

    # Overengineering you say?
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

    pck.start(opts.work_dir, opts.io_dir)

    rpc_server = _create_rpc_server(pck, opts)

    pck.join()
    rpc_server.shutdown()
    rem.delayed_executor.stop() # TODO FIXME Race-condition (can run some in pck)

# TODO

    with open(opts.result_file, 'w') as out:
        pass # TODO
