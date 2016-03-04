import sys
import threading
import random
import time
import logging
import os

sys.path.append('.')
import rem_server
import rem.connmanager
import rem.scheduler
import rem.job
import rem.osspec
from rem.packet import PacketState

sys.path.append(
    os.path.abspath(os.path.dirname(rem_server.__file__)) + '/client')

rem.osspec.reg_signal_handler = lambda *args: None

#def create_daemon(work_dir):
    #ctx = rem_server.DefaultContext('start')
    ##rem_server._context = ctx

    #rem_server._init_fork_locking(ctx)

    #sched = rem_server.CreateScheduler(ctx)
    #rem_server._scheduler = sched

    #return rem_server.RemDaemon(sched, ctx)

import tempfile
import shutil

class NamedTemporaryDir(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        self.name = tempfile.mkdtemp(*self._args, **self._kwargs)
        print >>sys.stderr, self.name
        return self.name

    def __exit__(self, e, t, bt):
        shutil.rmtree(self.name)
        self.name = None

def produce_config(out, work_dir, hostname):
    print >>out, """
[DEFAULT]
project_dir = {project_dir}

[store]
pck_dir = %(project_dir)s/packets
recent_tags_file = %(project_dir)s/backups/recent_tags.db
tags_db_file = %(project_dir)s/backups/tags.db
remote_tags_db_file = %(project_dir)s/backups/tags-remote.db
backup_dir = %(project_dir)s/backups
backup_period = 300
backup_count = 10
backup_child_max_working_time = 900
journal_lifetime = 3600
binary_dir = %(project_dir)s/bin
binary_lifetime = 86400
error_packet_lifetime = 604800
success_packet_lifetime = 259200
cloud_tags_server = localhost:17773
cloud_tags_masks = file://%(project_dir)s/cloud_tags.masks

[log]
dir = %(project_dir)s/log
warnlevel = debug
filename = rem.log
rollcount = 8

[run]
setup_script = %(project_dir)s/setup_env.sh
poolsize = 100
xmlrpc_poolsize = 20
readonly_xmlrpc_poolsize = 10

[server]
port = 8104
readonly_port = 8103
system_port = 8105
network_topology = local://%(project_dir)s/network_topology.cfg
network_hostname = {network_hostname}
send_emails = yes
send_emergency_emails = no
use_memory_profiler = no
""".format(
        project_dir=work_dir,
        network_hostname=hostname,
    )

def create_scheduler(work_dir):
    config_filename = work_dir + "/rem.cfg"

    with open(config_filename, "w") as conf:
        produce_config(conf, work_dir, hostname='foobar')

    import rem.context
    ctx = rem.context.Context(config_filename, "start")

    rem_server._init_fork_locking(ctx)

    #import json
    #print json.dumps(ctx.__dict__, indent=3)

    with open(work_dir + '/cloud_tags.masks', 'w') as out:
        print >>out, '_cloud_.*'

    with open(work_dir + '/network_topology.cfg', 'w') as out:
        print >>out, '[servers]'
        print >>out, 'foobar = http://localhost:8884, http://localhost:8885'

    return rem_server.CreateScheduler(ctx)

class Scheduler(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._sched = None

    def __enter__(self):
        self._sched = create_scheduler(*self._args, **self._kwargs)
        self._sched.Start()
        return self._sched

    def __exit__(self, e, t, bt):
        sched = self._sched
        self._sched = None
        sched.Stop()

if __name__ == '__main__':
    #print >>sys.stderr, 'helpers.py pid =', os.getpid()
    #sched, join_daemon = start(with_daemon=True)
    #join_daemon()

    def remove_if(dir, cond):
        for item in os.listdir(dir):
            if cond(item):
                os.unlink(dir + '/' + item)

    def remove_backups(work_dir):
        remove_if(work_dir + '/backups', lambda file : file.startswith('sched-'))

    def remove_journal(work_dir):
        remove_if(work_dir + '/backups', lambda file : file.startswith('recent_tags.db'))

    print >>sys.stderr, os.getpid()

    def test_01(do_intermediate_backup=False,
                do_final_backup=False,
                do_remove_journal=False,
                do_remove_backups=False):

        print do_intermediate_backup, do_final_backup, do_remove_journal, do_remove_backups

        def get_updates():
            return sched.tagRef._safe_cloud.get_state_updates()

        with NamedTemporaryDir(prefix='remd-') as work_dir:
            with Scheduler(work_dir) as sched:
                tags = sched.tagRef

                assert get_updates() == []

                tags.AcquireTag('_cloud_tag_01').Reset('message01')
                assert len(get_updates()) == 1

                if do_intermediate_backup:
                    sched.RollBackup()
                    print os.listdir(work_dir + '/backups')

                tags.AcquireTag('_cloud_tag_02').Reset('message02')

                all_updates = get_updates()
                assert len(all_updates) == 2

                if do_final_backup:
                    sched.RollBackup()
                    print os.listdir(work_dir + '/backups')

            if do_remove_journal:
                remove_journal(work_dir)

            if do_remove_backups:
                remove_backups(work_dir)

            print "-----------------------------------------------"
            with Scheduler(work_dir) as sched:
                print os.listdir(work_dir + '/backups')
                updates = get_updates()

                if do_remove_journal and do_remove_backups:
                    assert updates == []

                elif do_remove_backups:
                    assert updates == all_updates

                elif do_remove_journal:
                    if do_final_backup:
                        assert updates == all_updates
                    elif do_intermediate_backup:
                        assert updates == all_updates[0:1]
                    else:
                        assert updates == []

    #test_01(True, False, True, False)
    #sys.exit()

    for do_intermediate_backup in [True, False]:
        for do_final_backup in [True, False]:
            for do_remove_journal in [True, False]:
                for do_remove_backups in [True, False]:
                    test_01(
                        do_intermediate_backup=do_intermediate_backup,
                        do_final_backup=do_final_backup,
                        do_remove_journal=do_remove_journal,
                        do_remove_backups=do_remove_backups,
                    )
