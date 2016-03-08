import sys
import time
import logging
import os

import tempfile
import shutil

import unittest

import rem_server
import rem.context
from rem.callbacks import ETagEvent

LEAVE_WORK_DIR = bool(os.getenv('LEAVE_WORK_DIR', False))

class NamedTemporaryDir(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        self.name = tempfile.mkdtemp(*self._args, **self._kwargs)
        if LEAVE_WORK_DIR:
            print >>sys.stderr, 'working directory:', self.name
        return self.name

    def __exit__(self, e, t, bt):
        if not LEAVE_WORK_DIR:
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
cloud_tags_server = no-such-domain-ldfkgjsghkjgfdkjgfdkj:1023
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

    ctx = rem.context.Context(config_filename, "start")

    rem_server._init_fork_locking(ctx)

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

def remove_if(dir, cond):
    for item in os.listdir(dir):
        if cond(item):
            os.unlink(dir + '/' + item)

def remove_backups(work_dir):
    remove_if(work_dir + '/backups', lambda file : file.startswith('sched-'))

def remove_journal(work_dir):
    remove_if(work_dir + '/backups', lambda file : file.startswith('recent_tags.db'))

def testVrs(self,
            do_intermediate_backup=False,
            do_final_backup=False,
            do_remove_journal=False,
            do_remove_backups=False):

    #print do_intermediate_backup, do_final_backup, do_remove_journal, do_remove_backups

    def check_uniq(iterable):
        seen = set()
        for v in iterable:
            if v in seen:
                raise ValueError("%s duplicated" % v)
            seen.add(v)

    def get_updates():
        ret = sched.tagRef._safe_cloud.get_state_updates()
        check_uniq(id for id, _ in ret)
        return [update for _, update in ret]

    def backup():
        sched.RollBackup()
        time.sleep(1.5) # hack for same-timestamp-in-journal-filename problem

    first_part = [
        ('_cloud_tag_01', ETagEvent.Reset, 'message01'),
        ('_cloud_tag_02', ETagEvent.Reset, ''),
        ('_cloud_tag_01', ETagEvent.Set, None),
        ('_cloud_tag_01', ETagEvent.Unset, None),
    ]

    second_part = [
        ('_cloud_tag_02', ETagEvent.Set, None),
        ('_cloud_tag_02', ETagEvent.Set, None),
        ('_cloud_tag_01', ETagEvent.Reset, 'message02'),
        ('_cloud_tag_02', ETagEvent.Unset, None),
        ('_cloud_tag_01', ETagEvent.Set, None),
    ]

    all_updates = first_part + second_part

    def apply_updates(updates):
        for tag, ev, msg in updates:
            tags.AcquireTag(tag).Modify(ev, msg)

    with NamedTemporaryDir(prefix='remd-') as work_dir:
        with Scheduler(work_dir) as sched:
            tags = sched.tagRef

            self.assertEqual(get_updates(), [])

            apply_updates(first_part)

            self.assertEqual(get_updates(), first_part)

            if do_intermediate_backup:
                backup()

            apply_updates(second_part)

            self.assertEqual(get_updates(), all_updates)

            if do_final_backup:
                backup()

        if do_remove_journal:
            remove_journal(work_dir)

        if do_remove_backups:
            remove_backups(work_dir)

        with Scheduler(work_dir) as sched:
            updates = get_updates()

            if do_remove_journal and do_remove_backups:
                self.assertEqual(updates, [])

            elif do_remove_backups:
                self.assertEqual(updates, all_updates)

            elif do_remove_journal:
                if do_final_backup:
                    self.assertEqual(updates, all_updates)
                elif do_intermediate_backup:
                    self.assertEqual(updates, first_part)
                else:
                    self.assertEqual(updates, [])


class T18(unittest.TestCase):
    """Test rem.storages.SafeCloud backup and journal"""

    def testSafeCloudBackupAndJournalling(self):
        for do_intermediate_backup in [True, False]:
            for do_final_backup in [True, False]:
                for do_remove_journal in [True, False]:
                    for do_remove_backups in [True, False]:
                        l = locals()
                        setup = {
                            name: l[name] for name in [
                                'do_intermediate_backup',
                                'do_final_backup',
                                'do_remove_journal',
                                'do_remove_backups',
                            ]
                        }
                        logging.debug(setup)
                        testVrs(self, **setup)

