import os
import sys
import time
import bsddb3
from threading import Thread
from multiprocessing import Queue as InterProcessQueue, Process, Value as SharedValue
from Queue import Queue as InterThreadQueue
import cPickle as pickle

from rem_logging import logger as logging
import cloud_client
import cloud_connection
from callbacks import LocalTag, CloudTag, ETagEvent


STOP_INDICATOR = '__STOP__'


try:
    import prctl
    def set_thread_name(name):
        prctl.set_name(name)
except ImportError:
    def set_thread_name(name):
        pass


def _read_db(db_filename, bucket_size, output_queue, read_count):
    set_thread_name("rem-read_db")

    inner_output_queue = InterThreadQueue()

# TODO Remove. This thread is useless. InterProcessQueue.put() is not locked here actually
    def send_worker():
        while True:
            bucket = inner_output_queue.get()
            if bucket == STOP_INDICATOR:
                return

            read_count.value += len(bucket)
            output_queue.put(bucket)

    send_thread = Thread(target=send_worker)
    send_thread.start()

    bucket_holder = [[]]
    def flush():
        bucket = bucket_holder[0]
        if bucket:
            logging.debug('DB >> %d' % len(bucket))
            inner_output_queue.put(bucket)
            bucket_holder[0] = []

    db = bsddb3.btopen(db_filename, "r")
    try:
        bucket = bucket_holder[0]

        for tag_name, tag_serialized in db.iteritems():
            bucket.append(tag_serialized)
            if len(bucket) == bucket_size:
                flush()
                bucket = bucket_holder[0]

        if bucket:
            flush()

        inner_output_queue.put(STOP_INDICATOR)
        send_thread.join()

        #for _ in xrange(stop_indicator_count):
            #output_queue.put(STOP_INDICATOR)
    finally:
        try:
            db.close()
        except:
            pass


def _write_to_yt(input_queue, output_queue, in_memory_tags, cloud_tags_server_addr,
                 to_yt_write_count, saved_count, active_yt_request_count):
    set_thread_name("rem-write_to_yt")

    #journal_events = []
    #def register_journal_event(ev):
        #journal_events.append(ev)

    cloud = cloud_client.Client(
        cloud_connection.from_description(cloud_tags_server_addr),
        lambda ev: None
    )

    inner_input_queue  = InterThreadQueue() # FIXME TODO
    #inner_input_queue  = InterThreadQueue(4) # FIXME TODO
    inner_output_queue = InterThreadQueue()

    def receive_worker():
        while True:
            bucket = input_queue.get()
            if bucket == STOP_INDICATOR:
                inner_input_queue.put(bucket)
                return

            yt_updates = []
            saved_records = []
            for tag_serialized in bucket:
                tag = pickle.loads(tag_serialized)
                tag_name = tag.GetFullname()

                if isinstance(tag, LocalTag) and tag_name not in in_memory_tags:
                    yt_updates.append((tag_name, ETagEvent.Set if tag.IsLocallySet() else ETagEvent.Unset))
                else:
                    saved_records.append((tag_name, tag_serialized))

            if yt_updates:
                #logging.debug('yt_updates(%d)' % len(yt_updates))
                inner_input_queue.put(yt_updates)

            if saved_records:
                with saved_count.get_lock():
                    saved_count.value += len(saved_records)
                #logging.debug('saved_records(%d)' % len(saved_records))
                output_queue.put(saved_records) # FIXME use some intermediate thread-queue?

    def send_worker():
        while True:
            events = inner_output_queue.get()
            if events == STOP_INDICATOR:
                return

            task = []
            #for tag_name, version, event in events:
            version = 1
            for tag_name, event in events:
                tag = CloudTag(tag_name, None)
                tag.version = version
                tag.done = event == ETagEvent.Set
                task.append((tag_name, pickle.dumps(tag, 2)))

            #with to_yt_write_count.get_lock():
                #to_yt_write_count.value += len(task)
            output_queue.put(task)

    recv_worker = Thread(target=receive_worker)
    send_worker = Thread(target=send_worker)
    recv_worker.start()
    send_worker.start()

    try:
        while True:
            t0 = time.time()
            updates = inner_input_queue.get()
            if updates == STOP_INDICATOR:
                break
            logging.debug('T[GET] = %.3f' % (time.time() - t0))

            done = cloud.update(updates) # cpu-bound to protobuf conversion
            with active_yt_request_count.get_lock():
                active_yt_request_count.value += 1
            # ...
            done.get()
            with active_yt_request_count.get_lock():
                active_yt_request_count.value -= 1

            logging.debug('%d >> YT' % len(updates)) # TODO FIXME get real tag_version

            with to_yt_write_count.get_lock():
                to_yt_write_count.value += len(updates)

            t0 = time.time()
            inner_output_queue.put(updates)
            #inner_output_queue.put([
                #(tag_name, 1, event)
                    #for tag_name, event in updates
            #])
            logging.debug('T[PUT] = %.3f' % (time.time() - t0))

        recv_worker.join()
        inner_output_queue.put(STOP_INDICATOR)
        send_worker.join()
    finally:
        try:
            cloud.stop()
        except:
            pass


def _update_db(db_filename, input_queue, write_count):
    set_thread_name("rem-update_db")

    db = bsddb3.btopen(db_filename, "c")
    try:
        while True:
            # FIXME 2 io-bound operations here?

            events = input_queue.get()
            if events == STOP_INDICATOR:
                return

            logging.debug('%d >> DB' % len(events))

            for tag_name, tag_serialized in events:
                db[tag_name] = tag_serialized

            write_count.value += len(events)
    finally:
        try:
            db.close()
        except:
            pass


def convert_on_disk_tags_to_cloud(db_filename, in_memory_tags, cloud_tags_server_addr):
    start_time = time.time()

    bucket_size = 10000
    yt_writer_count = 20

    to_yt_queue = InterProcessQueue()
    from_yt_queue = InterProcessQueue()

    db_read_count = SharedValue('i')
    yt_write_count = SharedValue('i')
    saved_count = SharedValue('i')
    db_write_count = SharedValue('i')
    active_yt_request_count = SharedValue('i')

    processes = set()

    yt_writers = [
        Process(
            target=_write_to_yt,
            args=(
                to_yt_queue,
                from_yt_queue,
                in_memory_tags,
                cloud_tags_server_addr,
                yt_write_count,
                saved_count,
                active_yt_request_count
            )
        )
            for _ in range(yt_writer_count)
    ]

    for p in yt_writers:
        p.start()

    db_reader = Process(target=_read_db, args=(db_filename, bucket_size, to_yt_queue, db_read_count))
    db_reader.start()

    new_db_filename = db_filename + '.new'
    if os.path.exists(new_db_filename):
        os.unlink(new_db_filename)

    db_writer = Process(target=_update_db, args=(new_db_filename, from_yt_queue, db_write_count))
    db_writer.start()

    processes = set([db_reader, db_writer] + yt_writers)

    print >>sys.stderr, "$ top " + ' '.join([
        '-p %d' % pid for pid in ([os.getpid()] + [p.pid for p in processes])
    ])

    def print_stats():
        print >>sys.stderr, '+ %.1f %10d<<db %10d>>save %10d>>yt %10d>>db %3d' % (
            time.time() - start_time,
            db_read_count.value,
            saved_count.value,
            yt_write_count.value,
            db_write_count.value,
            active_yt_request_count.value
        )

    def join(p):
        while p.exitcode is None:
            print_stats()
            p.join(1)

        processes.discard(p)
        if p.exitcode:
            for p in processes:
                p.terminate()
                p.join()
            raise RuntimeError()

    join(db_reader)
    for _ in range(yt_writer_count):
        to_yt_queue.put(STOP_INDICATOR)

    for p in yt_writers:
        join(p)

    from_yt_queue.put(STOP_INDICATOR)

    join(db_writer)
    print_stats()
