import sys
import time
import logging
import os

import tempfile
import shutil

import unittest

import rem_server
import rem.context
import rem.storages
from rem.callbacks import ETagEvent


LEAVE_WORK_DIR = bool(os.getenv('LEAVE_WORK_DIR', False))
CLOUD_TAGS_SERVER = 'nanny://47821a5802574cdf9e23db6d15ec9b0d@nanny.yandex-team.ru/test_rem_cloud_tags_proxy'


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
backup_period = 86400
backup_count = 10
backup_child_max_working_time = 900
journal_lifetime = 3600
binary_dir = %(project_dir)s/bin
binary_lifetime = 86400
error_packet_lifetime = 604800
success_packet_lifetime = 259200

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


def create_default_context(work_dir):
    config_filename = work_dir + "/rem.cfg"

    with open(config_filename, "w") as conf:
        produce_config(conf, work_dir, hostname='foobar')

    with open(work_dir + '/network_topology.cfg', 'w') as out:
        print >>out, '[servers]'
        print >>out, 'foobar = http://localhost:8884, http://localhost:8885'

    ret = rem.context.Context(config_filename)

    return ret


def create_scheduler(ctx):
    rem_server._init_fork_locking(ctx)
    rem_server.init_logging(ctx)

    return rem_server.create_scheduler(ctx)


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


def check_uniq(iterable):
    seen = set()
    for v in iterable:
        if v in seen:
            raise ValueError("%s duplicated" % v)
        seen.add(v)


def get_safecloud_updates(sched):
    ret = sched.tagRef._safe_cloud.get_state_updates()
    check_uniq(id for id, _ in ret)
    return [update for _, update in ret]


def apply_updates(sched, updates):
    for tag, ev, msg in updates:
        sched.tagRef.AcquireTag(tag).Modify(ev, msg)


def backup(sched):
    sched.RollBackup()
    time.sleep(1.5) # hack for same-timestamp-in-journal-filename problem


def run_common_safecloud_tests(self,
            do_intermediate_backup=False,
            do_final_backup=False,
            do_remove_journal=False,
            do_remove_backups=False):

    #print do_intermediate_backup, do_final_backup, do_remove_journal, do_remove_backups

    def get_updates():
        return get_safecloud_updates(sched)

    def _apply_updates(updates):
        apply_updates(sched, updates)

    def _backup():
        backup(sched)

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

    with NamedTemporaryDir(prefix='remd-') as work_dir:
        cloud_tags_masks_file = work_dir + '/cloud_tags.masks'

        with open(cloud_tags_masks_file, 'w') as out:
            print >>out, '_cloud_.*'

        ctx = create_default_context(work_dir)

        ctx.cloud_tags_server = 'no-such-domain-ldfkgjsghkjgfdkjgfdkj.:1023'
        ctx.cloud_tags_masks = 'file://' + cloud_tags_masks_file
        ctx.allow_startup_tags_conversion = False

        with Scheduler(ctx) as sched:
            self.assertEqual(get_updates(), [])

            _apply_updates(first_part)

            self.assertEqual(get_updates(), first_part)

            if do_intermediate_backup:
                _backup()

            _apply_updates(second_part)

            self.assertEqual(get_updates(), all_updates)

            if do_final_backup:
                _backup()

        if do_remove_journal:
            remove_journal(work_dir)

        if do_remove_backups:
            remove_backups(work_dir)

        ctx.Scheduler = None

        with Scheduler(ctx) as sched:
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


def CloudTag(name, is_set, version):
    return ('cloud', name, is_set, version)

def LocalTag(name, is_set):
    return ('local', name, is_set)

def RemoteTag(name, is_set):
    return ('remote', name, is_set)

def test_tag_from_real_tag(tag):
    if tag.IsCloud():
        f = lambda name, is_set: CloudTag(name, is_set, tag.version)
    elif tag.IsRemote():
        f = RemoteTag
    else:
        f = LocalTag

    return f(tag.GetFullname(), tag.IsLocallySet())

def tags_dict(*tags):
    return {t[1]: t for t in tags}

def tags_dict_from_sched(sched):
    return {
        tag_name: test_tag_from_real_tag(tag)
            for tag_name, tag in sched.tagRef.inmem_items.iteritems()
    }


class T18(unittest.TestCase):
    """Test rem.storages.SafeCloud backup and journal"""

    def testSafeCloudBackupAndJournalling(self):
        original_timeout = rem.storages.TagStorage.CLOUD_CLIENT_STOP_TIMEOUT
        rem.storages.TagStorage.CLOUD_CLIENT_STOP_TIMEOUT = 1.0

        try:
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
                            run_common_safecloud_tests(self, **setup)
        finally:
            rem.storages.TagStorage.CLOUD_CLIENT_STOP_TIMEOUT = original_timeout

    def testCloudTagsOnDiskWithoutCloudSetup(self):
        start_time = str(time.time())

        def tag_name(name):
            return name + '-' + start_time

        def get_updates():
            return get_safecloud_updates(sched)

        def _apply_updates(updates):
            apply_updates(sched, updates)

        def _backup():
            backup(sched)

        #all_updates = first_part + second_part

        def _tags_dict_from_sched():
            return tags_dict_from_sched(sched)

        with NamedTemporaryDir(prefix='remd-') as work_dir:
            #cloud_tags_masks_file = work_dir + '/cloud_tags.masks'

            #with open(cloud_tags_masks_file, 'w') as out:
                #print >>out, '_cloud_.*'

            ctx = create_default_context(work_dir)

            ctx.cloud_tags_release_delay = 0 # XXX

            ctx.cloud_tags_server = CLOUD_TAGS_SERVER
            ctx.all_tags_in_cloud = True
            #ctx.cloud_tags_masks = 'file://' + cloud_tags_masks_file
            ctx.allow_startup_tags_conversion = False

            with Scheduler(ctx) as sched:
                logging.debug('stage0')

                self.assertEqual(get_updates(), [])

                first_part = [
                    (tag_name('_cloud_tag_01'), ETagEvent.Reset, 'message01'),
                    (tag_name('_cloud_tag_02'), ETagEvent.Reset, ''),
                    (tag_name('_cloud_tag_01'), ETagEvent.Set, None),
                    (tag_name('_cloud_tag_01'), ETagEvent.Unset, None),
                ]

                _apply_updates(first_part)
                time.sleep(5) # ugly

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        CloudTag(tag_name('_cloud_tag_01'), False, 3),
                        CloudTag(tag_name('_cloud_tag_02'), False, 1),
                    )
                )

                _backup()

                self.assertEqual(_tags_dict_from_sched(), {})


            ctx.cloud_tags_server = None
            ctx.all_tags_in_cloud = False
            ctx.Scheduler = None

            with Scheduler(ctx) as sched:
                logging.debug('stage1')

                self.assertEqual(get_updates(), [])
                self.assertEqual(_tags_dict_from_sched(), {})

                second_part = [
                    (tag_name('_cloud_tag_02'), ETagEvent.Set, None),
                    (tag_name('_cloud_tag_02'), ETagEvent.Set, None),
                    (tag_name('_cloud_tag_01'), ETagEvent.Reset, 'message02'),
                    (tag_name('_cloud_tag_02'), ETagEvent.Unset, None),
                    (tag_name('_cloud_tag_01'), ETagEvent.Set, None),
                ]

                self.assertEqual(
                    rem.storages.SerialUpdateOnlyDummyCloudClient,
                    type(sched.tagRef._cloud)
                )

                _apply_updates(second_part)
                time.sleep(5) # ugly

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        CloudTag(tag_name('_cloud_tag_01'), False, 3),
                        CloudTag(tag_name('_cloud_tag_02'), False, 1),
                    )
                )

                _backup()

                self.assertEqual(_tags_dict_from_sched(), {})

            def vivify_tags(tags):
                for tag in tags:
                    sched.tagRef.AcquireTag(tag)

            ctx.cloud_tags_server = CLOUD_TAGS_SERVER
            ctx.all_tags_in_cloud = True
            ctx.Scheduler = None

            with Scheduler(ctx) as sched:
                logging.debug('stage2')

                self.assertEqual(get_updates(), second_part)
                self.assertEqual(_tags_dict_from_sched(), {})

                vivify_tags([
                    tag_name('_cloud_tag_01'),
                    tag_name('_cloud_tag_02'),
                ])

                time.sleep(5) # ugly

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        CloudTag(tag_name('_cloud_tag_01'), True,  5),
                        CloudTag(tag_name('_cloud_tag_02'), False, 4),
                    )
                )

    def testToCloudStartupConversion(self):
        start_time = str(time.time())

        def tag_name(name):
            return name + '-' + start_time

        def get_updates():
            return get_safecloud_updates(sched)

        def _apply_updates(updates):
            apply_updates(sched, updates)

        def _backup():
            backup(sched)

        def _tags_dict_from_sched():
            return tags_dict_from_sched(sched)

        with NamedTemporaryDir(prefix='remd-') as work_dir:
            cloud_tags_masks_file = work_dir + '/cloud_tags.masks'

            with open(cloud_tags_masks_file, 'w') as out:
                print >>out, 'tag_02-.*'

            ctx = create_default_context(work_dir)

            ctx.cloud_tags_release_delay = 0 # XXX

            ctx.cloud_tags_server = CLOUD_TAGS_SERVER
            ctx.cloud_tags_masks = 'file://' + cloud_tags_masks_file

            with Scheduler(ctx) as sched:
                logging.debug('stage0')

                self.assertEqual(get_updates(), [])

                first_part = [
                    (tag_name('tag_%02d' % idx), ETagEvent.Set, None) for idx in range(4)
                ]

                _apply_updates(first_part)
                time.sleep(5) # ugly

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        LocalTag(tag_name('tag_00'), True),
                        LocalTag(tag_name('tag_01'), True),
                        CloudTag(tag_name('tag_02'), True, 1),
                        LocalTag(tag_name('tag_03'), True),
                    )
                )

                wait_tags = [
                    sched.tagRef.AcquireTag(tag_name('tag_%02d' % idx))
                        for idx in range(5)
                ]

                pck = rem.packet.LocalPacket('foobar', 0, ctx, [], wait_tags)

                sched.AddPacketToQueue('q', pck)

                del pck

                _backup()

            with open(cloud_tags_masks_file, 'w') as out:
                print >>out, 'tag_00-.*'
                print >>out, 'tag_02-.*'

            ctx.allow_startup_tags_conversion = False
            ctx.Scheduler = None

            with Scheduler(ctx) as sched:
                logging.debug('stage1')

                time.sleep(5) # ugly

                self.assertEqual(get_updates(), [])

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        LocalTag(tag_name('tag_00'), True),
                        LocalTag(tag_name('tag_01'), True),
                        CloudTag(tag_name('tag_02'), True, 1),
                        LocalTag(tag_name('tag_03'), True),
                        LocalTag(tag_name('tag_04'), False),
                    )
                )

                _backup()

            ctx.allow_startup_tags_conversion = True
            ctx.Scheduler = None

            with Scheduler(ctx) as sched:
                logging.debug('stage2')

                time.sleep(5) # ugly

                self.assertEqual(get_updates(), [])

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        CloudTag(tag_name('tag_00'), True, 1),
                        LocalTag(tag_name('tag_01'), True),
                        CloudTag(tag_name('tag_02'), True, 1),
                        LocalTag(tag_name('tag_03'), True),
                        LocalTag(tag_name('tag_04'), False),
                    )
                )

                _backup()

            ctx.allow_startup_tags_conversion = True
            ctx.all_tags_in_cloud = True
            ctx.Scheduler = None

            self.maxDiff = None
            with Scheduler(ctx) as sched:
                logging.debug('stage3')

                time.sleep(5) # ugly

                self.assertEqual(get_updates(), [])

                self.assertEqual(
                    _tags_dict_from_sched(),
                    tags_dict(
                        CloudTag(tag_name('tag_00'), True,  1),
                        CloudTag(tag_name('tag_01'), True,  1),
                        CloudTag(tag_name('tag_02'), True,  1),
                        CloudTag(tag_name('tag_03'), True,  1),
                        CloudTag(tag_name('tag_04'), False, 0),
                    )
                )

                _backup()
