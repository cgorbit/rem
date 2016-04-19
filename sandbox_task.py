#! /usr/bin/env python

import rem.job as job
import rem.packet as packet
import rem.delayed_executor as delayed_executor
import rem.sandbox_packet

import rem.context
import rem.scheduler
import rem.constants as constants

import hashlib
def calc_checksum(path):
    BUF_SIZE = 256 * 1024
    with open(path, "r") as reader:
        cs_calc = hashlib.md5()
        while True:
            buff = reader.read(BUF_SIZE)
            if not buff:
                break
            cs_calc.update(buff)
        return cs_calc.hexdigest()

if __name__ == '__main__':
    ctx = rem.context.Context('/home/trofimenkov/rem/rem.cfg')
    sched = rem.scheduler.Scheduler(ctx)

    def pck_add_job(pck, shell, parents=None, pipe_parents=None, set_tag=None, tries=5,
                    max_err_len=None, retry_delay=None, pipe_fail=False,
                    description="", notify_timeout=constants.NOTIFICATION_TIMEOUT,
                    max_working_time=constants.KILL_JOB_DEFAULT_TIMEOUT,
                    output_to_status=False):

        if parents is None:
            parents = []

        if pipe_parents is None:
            pipe_parents = []

        job = pck.rpc_add_job(shell, parents, pipe_parents,
                              set_tag and sched.tagRef.AcquireTag(set_tag),
                              tries, max_err_len, retry_delay, pipe_fail,
                              description, notify_timeout, max_working_time,
                              output_to_status)
        return job.id

    def save_binary(raw):
        sched.binStorage.CreateFile(raw)

    def pck_add_binary(pck, name, fullpath):
        checksum = calc_checksum(fullpath)

        file = sched.binStorage.GetFileByHash(checksum)
        if not file:
            with open(fullpath) as in_:
                save_binary(in_.read())
        file = sched.binStorage.GetFileByHash(checksum)
        assert bool(file)

        pck.rpc_add_binary(name, file)


    pck = packet.SandboxPacket(
        'test_sandbox_packet_name_001',
        priority=0,
        context=ctx,
        notify_emails=[],
        wait_tags=[],
        set_tag=None,
        kill_all_jobs_on_error=True,
        is_resetable=True,
        notify_on_reset=True,
        notify_on_skipped_reset=True,
    )

    pck_add_job(pck, './true', tries=1)
    #pck_add_job(pck, './false', tries=1)
    pck_add_job(pck, 'sleep 4', tries=3)
    pck_add_job(pck, 'sleep 8', tries=3)

    pck_add_binary(pck, 'true', '/bin/true')

    sbx_pck = pck.create_sandbox_packet()

    from pprint import pprint
    pprint(sbx_pck.__dict__)
    print
    from cStringIO import StringIO
    out = StringIO()
    sbx_pck.serialize(out)
    serialized = out.getvalue()

    print serialized

    sbx_pck.deserialize(StringIO(serialized))

    #delayed_executor.start()
    #pass
    #delayed_executor.stop()
