import os
import sys
import time
import threading
import logging

import rem.cloud_client
from rem.cloud_client import ETagEvent
rem.cloud_client.COUNT_IO = True


def _stat_worker(client, iter_count, call_count, done_count, event_count, should_stop):
    prev_call_count = 0
    prev_done_count = 0
    prev_event_count = 0
    prev_t = time.time()
    prev_io_stats = (0, 0)
    from sys import stderr

    while not(should_stop[0] and prev_done_count == iter_count):
        time.sleep(1.0)
        t = time.time()
        elapsed = t - prev_t
        io_stats = client.get_io_stats()

        call_delta = call_count[0] - prev_call_count
        done_delta = done_count[0] - prev_done_count
        event_delta = event_count[0] - prev_event_count

        io_delta = (
            io_stats[0] - prev_io_stats[0],
            io_stats[1] - prev_io_stats[1],
        )

        prev_call_count += call_delta
        prev_done_count += done_delta
        prev_event_count += event_delta
        prev_t = t
        prev_io_stats = io_stats

        setups = [
            ('call', call_delta, prev_call_count),
            ('done', done_delta, prev_done_count),
            ('event', event_delta, prev_event_count),
            ('read',  io_delta[0], io_stats[0]),
            ('write', io_delta[1], io_stats[1]),
        ]

        logging.debug('')
        for name, delta, total in setups:
            print >>stderr, '%s: %.1f (%.1fs, %d)' % \
                (name, delta / elapsed, elapsed, total)
        print >>stderr, client
        print >>stderr


def run_stat_thread(*args):
    t = threading.Thread(target=_stat_worker, args=args)
    t.start()
    return t.join


def execute(iter_count, client, code):
    should_stop = [False]

    call_count = [0]
    done_count = [0]
    event_count = [0]

    def inc_done(_):
        done_count[0] += 1

    def on_event(ev):
        event_count[0] += 1

    client._on_event = on_event

    join_stat = run_stat_thread(
        client, iter_count, call_count, done_count, event_count, should_stop)

    for idx in xrange(iter_count):
        tag = 'cluster=redwood_publish_use_%s_%s' % (EPOCH, idx)
        done = code(tag)
        call_count[0] += 1
        done.subscribe(inc_done)
        if idx % 5 == 0:
            time.sleep(0.001)

    should_stop[0] = True
    join_stat()


def subscribe_tags(iter_count, client):
    execute(iter_count, client, lambda tag: client.subscribe(tag))


def set_tags(iter_count, client):
    execute(iter_count, client, lambda tag: client.serial_update((tag, ETagEvent.Set, None)))


if __name__ == '__main__':
    EPOCH = '%d' % time.time()
    iter_count = 30 * 1000

    server_addr = sys.argv[1]
    print >>sys.stderr, os.getpid()

    sys.stdin.readline()

    def run(tests, before_end=None):
        client = rem.cloud_client.getc(server_addr, lambda ev: None)

        for test in tests:
            logging.info('run test: %s' % test.__name__)
            sys.stdin.readline()
            test(iter_count, client)

        if before_end:
            before_end()

        print >>sys.stderr, "all finished"
        sys.stdin.read()

        client.stop()

    #run([subscribe_tags])
    #run([set_tags, subscribe_tags], lambda : run([subscribe_tags]))
    #run([subscribe_tags, set_tags])

    run([set_tags, subscribe_tags])
    #run([set_tags])

