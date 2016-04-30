#! /usr/bin/env python

import os
import cPickle as pickle
import argparse
import base64

import rem.sandbox_packet


def parse_arguments():
    p = argparse.ArgumentParser()

    p.add_argument('--io-dir', dest='io_dir', required=True)
    p.add_argument('--work-dir', dest='work_dir', required=True)
    p.add_argument('--custom-resources', dest='custom_resources')

    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--snapshot-data', dest='snapshot_data')
    group.add_argument('--snapshot-file', dest='snapshot_file')

    return p.parse_args()


if __name__ == '__main__':
    opts = parse_arguments()

    # Overengineering you say?
    for attr in ['io_dir', 'work_dir'] \
            + (['snapshot_file'] if opts.snapshot_file is not None else []):
        setattr(opts, attr, os.path.abspath(getattr(opts, attr)))


    #opts.snapshot_data = 'gAJjcmVtLnBhY2tldApKb2JHcmFwaApxASmBcQJ9cQMoVRZraWxsX2FsbF9qb2JzX29uX2Vycm9ycQSIVQRqb2JzcQV9cQZJMTQwMzA4MTkzMDIxMDA4CmNyZW0uam9iCkpvYgpxBymBcQh9cQkoVQZpbnB1dHNxCl1xC1UQbWF4X3dvcmtpbmdfdGltZXEMSgB1EgBVBXNoZWxscQ1VB3NsZWVwIDVxDlULZGVzY3JpcHRpb25xD1UAVQdyZXN1bHRzcRBdcRFVBXRyaWVzcRJLAFUJcGlwZV9mYWlscROJVQ1tYXhfdHJ5X2NvdW50cRRLBVUObm90aWZ5X3RpbWVvdXRxFUqAOgkAVQtyZXRyeV9kZWxheXEWTlUCaWRxF0kxNDAzMDgxOTMwMjEwMDgKVRBvdXRwdXRfdG9fc3RhdHVzcRiJVQttYXhfZXJyX2xlbnEZTlUHcGFyZW50c3EaXXEbVRNjYWNoZWRfd29ya2luZ190aW1lcRxLAFUGcGNrX2lkcR1VCnBjay1GRHhkX1BxHnVic3ViLg=='

    if opts.snapshot_file is not None:
        with open(opts.snapshot_file) as snap:
            pck = pickle.load(snap)
    else:
        pck = pickle.loads(base64.b64decode(opts.snapshot_data))

    #print pck.jobs[140308193021008].__dict__
    pck.start(opts.work_dir, opts.io_dir)
    pck.join()
