#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import socket
import threading
from collections import deque
import json
import errno

from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _EncodeVarint as EncodeVarint

from future import Promise, CheckAllFuturesSucceed
from common import split_in_groups
from profile import ProfiledThread
import cloud_tags_pb2
from rem_logging import logger as logging

READY_ACK_FUTURE = Promise().set(None).to_future()
READY_EMPTY_DICT_FUTURE = Promise().set({}).to_future()

COUNT_IO = bool(os.getenv('COUNT_CLOUD_TAGS_IO'))

class ServiceStopped(RuntimeError):
    pass

class StreamLikeBuffer(object):
    def __init__(self, stream):
        self._stream = stream
        self._buffer = ''

    class IndexError(IndexError):
        def __init__(self, msg, index):
            super(type(self), self).__init__(msg)
            self.index = index

    def __getitem__(self, idx):
        to_read = idx - (len(self._buffer) - 1)

        if to_read > 0:
            self._buffer += self._stream.read(to_read)

            if len(self._buffer) - 1 != idx:
                raise self.IndexError("EOF", idx)

        return self._buffer[idx]

def DecodeVarint32(stream):
    try:
        return _DecodeVarint32(StreamLikeBuffer(stream), 0)[0]
    except StreamLikeBuffer.IndexError as e:
        if e.index == 0:
            return None
        else:
            raise

class ETagEvent(object):
    Unset = 0
    Set   = 1
    Reset = 2

TagEventName = {
    ETagEvent.Unset: 'unset',
    ETagEvent.Set:   'set',
    ETagEvent.Reset: 'reset'
}

def asjsonstring(data):
    #return json.dumps(data, indent=3)
    return json.dumps(data)


class _CountedIO(object):
    def __init__(self, backend):
        self._backend = backend
        self.read_counter = 0
        self.write_counter = 0

    def read(self, size):
        ret = self._backend.read(size)
        self.read_counter += len(ret)
        return ret

    def write(self, str):
        self.write_counter += len(str)
        return self._backend.write(str)

    def close(self):
        self._backend.close()

    def flush(self):
        self._backend.flush()


class ProtobufConnection(object):
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sock = socket.create_connection((host, port))._sock
        self._in  = self._sock.makefile('r')
        self._out = self._sock.makefile('w')
        self._closed = False

        if COUNT_IO:
            self._in  = _CountedIO(self._in)
            self._out = _CountedIO(self._out)

        self._red_msg_count = 0
        self._written_msg_count = 0

    def get_stats(self):
        if hasattr(self._in, 'read_counter'):
            return self._in.read_counter, self._out.write_counter
        else:
            return None, None

    def __repr__(self):
        return '<%s ProtobufConnection to %s:%s %s at 0x%x>' % (
            'closed' if self._closed else 'open',
            self._host, self._port,
            self.get_stats(),
            id(self)
        )

    def __str__(self):
        return repr(self)

    def send(self, msg):
        out = self._out

        assert msg.IsInitialized() # FIXME
        #if not msg.IsInitialized():
            #raise RuntimeError("Message is not initialized")

        serialized = msg.SerializeToString()

        EncodeVarint(out.write, len(serialized))
        out.write(serialized)

        out.flush()

        self._written_msg_count += 1

    def recv(self):
        in_ = self._in

        size = DecodeVarint32(in_)

        if size is None: # EOF
            return None

        if not size:
            raise RuntimeError()

        serialized = in_.read(size)
        if len(serialized) != size:
            raise RuntimeError("EOF")

        msg = cloud_tags_pb2.TServerMessage()
        msg.ParseFromString(serialized)

        if not msg.IsInitialized():
            raise RuntimeError("Message from server not IsInitialized")

        self._red_msg_count += 1

        return msg

    def close(self):
# TODO kosher impl
        if hasattr(self, '_in'):
            self._in.close()
        if hasattr(self, '_out'):
            self._out.close()
        if hasattr(self, '_sock'):
            self._sock.close()
        self._closed = True

    def shutdown(self, how):
        self._sock.shutdown(how)

    def __del__(self):
        self.close()


class _FakePromise(object):
    def set(self, val=None, exc=None):
        pass


def _make_protobuf_connection((host, port)):
    #logging.debug('before ProtobufConnection(%s, %s)' % (host, port))
    ret = ProtobufConnection(host, port)
    #logging.debug('after ProtobufConnection(%s, %s)' % (host, port))
    return ret


class Client(object):
    MESSAGE_MAX_ITEM_COUNT = 100000 # ATW CodedInputStream::GetTotalBytesLimit() == 67_108_864
    __FAKE_PROMISE = _FakePromise()

    _ST_NONE   = 0
    _ST_WAIT   = 1
    _ST_NOWAIT = 2

    def __init__(self, connection_ctor_ctor, on_event):
        self._connection_constructor = connection_ctor_ctor(_make_protobuf_connection)
        self._on_event = on_event

        self._stopped = False

        self._outgoing = deque()
        self._running = {}
        self._subscriptions = set()

        self._next_message_id = 1
        self._should_stop = self._ST_NONE

        self._io = None
        self._io_stats = [0, 0]

        self._lock = threading.Lock()
        self._outgoing_not_empty = threading.Condition(self._lock)
        self._outgoing_empty = threading.Condition(self._lock)
        #self._running_empty = threading.Condition(self._lock)
        self._should_and_can_stop_cond = threading.Condition(self._lock)
        #self._connection_state_changed = threading.Condition(self._lock)
        #self._should_stop_cond = threading.Condition(self._lock)

        self._connect_thread = ProfiledThread(target=self._connect_loop, name_prefix='CldTg-Connect')
        self._connect_thread.start()

    def get_io_stats(self):
        conn = self._io._connection if self._io and self._io._connection else None

        msg_stat = (
            conn._red_msg_count if conn else None,
            conn._written_msg_count if conn else None)

        if not COUNT_IO:
            return (None, None) + msg_stat

        prev_stats = self._io_stats
        cur_stats = conn.get_stats() if conn else (0, 0)

        return (prev_stats[0] + cur_stats[0],
                prev_stats[1] + cur_stats[1]) + msg_stat

    def __repr__(self):
        with self._lock:
            io = self._io
            conn = io._connection if io and io._connection else None

            return '<%s.%s %s%s %s at 0x%x>' % (
                self.__module__,
                type(self).__name__,
                'stopped ' if self._stopped else '',
                '%s:%s %s' % (conn._host, conn._port, self.get_io_stats()) if conn else None,
                'running=%d, outgoing=%d, subs=%d' % (
                    len(self._running),
                    len(self._outgoing),
                    len(self._subscriptions)
                ),
                id(self)
            )

    class _Task(object):
        def __init__(self, promise, msg, is_resend):
            self.promise = promise
            self.msg = msg
            self.is_resend = is_resend

    def _create_connection(self):
        now = time.time()
        timeout = 0.0

        while True:
            with self._lock:
                if self._should_and_can_stop():
                    return

                self._should_and_can_stop_cond.wait(timeout)

                if self._should_and_can_stop():
                    return

            try:
                conn = self._connection_constructor()
            except Exception as e:
                logging.warning("Failed to connect: %s" % e)

            if conn:
                return conn

            timeout = 1.0

    def _connect_loop(self):
        self._connect_loop_inner()

        self._outgoing.clear()

        try:
            raise ServiceStopped()
        except ServiceStopped:
            exc = sys.exc_info()

        with self._lock:
            for task in self._running.itervalues():
                task.promise.set(exc=exc)
            self._running.clear()
            #self._running_empty.notify_all()

    def _connect_loop_inner(self):
        while True:
            with self._lock:
                if self._should_and_can_stop():
                    return

            if self._io:
                self._io._connection.close()
                if COUNT_IO:
                    rd, wr = self._io._connection.get_stats()
                    self._io_stats[0] += rd
                    self._io_stats[1] += wr
                self._io = None

            self._reconstruct_outgoing()

            # FIXME What if exception will throw here? self._broken? _push -> raise and fail futures

            io = self._io = self.IO()

            logging.info("Connecting to servers...")

            connection = self._create_connection()

            if not connection:
                break

            addr = (connection._host, connection._port)

            logging.info("Connected to %s" % (addr,))

            io._connection = connection

            with self._lock:
                self._io._connected = True
                #self._connection_state_changed.notify_all()

            io_threads = [
                ProfiledThread(target=self._read_loop, name_prefix='CldTg-ReadLoop'),
                ProfiledThread(target=self._write_loop, name_prefix='CldTg-WriteLoop')
            ]

            for t in io_threads:
                t.start()

            for t in io_threads:
                t.join()

            logging.info("Disconnected from %s" % (addr,))

    def _reconstruct_outgoing(self):
        with self._lock:
            resend_ids = [id for id, task in self._running.iteritems() if task.is_resend]

            for id in resend_ids:
                self._running.pop(id)

            self._outgoing = deque(
                self._running[id]
                    for id in sorted(self._running.keys())
            )

            # XXX _subscriptions после _outgoing, чтобы не приходили события по
            # тем тегам, на которые пользователь Client не подписан (ну, типа
            # инвариант)

            # XXX For now subscribes for already subscribed tags will not be
            # trigger events in journal

            # FIXME Global split feature in all _push callers?

            for tags in split_in_groups(self._subscriptions, self.MESSAGE_MAX_ITEM_COUNT):
                self._do_push(
                    self._create_subscribe_message(tags),
                    self.__FAKE_PROMISE,
                    is_resend=True
                )

    class IO(object):
        def __init__(self):
            self._thread_count = 2
            self._bye_received = False
            self._read_finished = False
            self._connection = None
            self._connected = False

    def _create_subscribe_message(self, tags):
        ret = cloud_tags_pb2.TClientMessage()
        ret.Subscribe.Tags.extend(tags)
        return ret

    def fetch_my_subscriptions(self):
        msg = cloud_tags_pb2.TClientMessage()
        msg.Debug.GetMySubscriptions = True; # TODO Use TGetMySubscriptions instead of Debug
        return self._push(msg)

    def ping(self):
        msg = cloud_tags_pb2.TClientMessage()

        ping = msg.Ping
        ping.SetInParent()

        return self._push(msg)

    def subscribe(self, tags, with_future=True):
        return self._sub_unsub_in_groups(tags, self._do_subscribe, with_future)

    def unsubscribe(self, tags, with_future=True):
        return self._sub_unsub_in_groups(tags, self._do_unsubscribe, with_future)

    def _do_subscribe(self, tags):
        def update():
            self._subscriptions |= tags

        return self._push(self._create_subscribe_message(tags), code=update)

    def _do_unsubscribe(self, tags):
        msg = cloud_tags_pb2.TClientMessage()
        msg.Unsubscribe.Tags.extend(tags)

        def update():
            self._subscriptions -= tags

        return self._push(msg, code=update)

    def _sub_unsub_in_groups(self, tags, method, with_future=True):
        if isinstance(tags, str):
            tags = set([tags])

        if not isinstance(tags, set):
            tags = set(tags)

        def fix_result(f):
            return f if with_future else None

        if not tags:
            return fix_result(READY_ACK_FUTURE)

        if len(tags) < self.MESSAGE_MAX_ITEM_COUNT:
            return fix_result(method(tags))

        futures = [
            method(set(tags_group))
                for tags_group in split_in_groups(tags, self.MESSAGE_MAX_ITEM_COUNT)
        ]

        if not with_future:
            return

        return CheckAllFuturesSucceed(futures)

    def lookup(self, tags):
        if not isinstance(tags, set):
            tags = set(tags)

        if not tags:
            return READY_EMPTY_DICT_FUTURE

        msg = cloud_tags_pb2.TClientMessage()
        msg.Lookup.Tags.extend(tags)
        return self._push(msg)

    def match(self, prefix=None, regexp=None, limit=None):
        msg = cloud_tags_pb2.TClientMessage()

        match = msg.Match
        match.SetInParent()

        if prefix is not None:
            match.Prefix = prefix
        if regexp is not None:
            match.Regexp = regexp
        if limit is not None:
            match.Limit = limit

        return self._push(msg)

    @staticmethod
    def _form_update_item(item, update):
        item.TagName = update[0]
        item.Event = update[1] # FIXME convert explicitly
        if len(update) > 2 and update[2] is not None:
            item.Comment = update[2]

    # If this function will split updates into buckets, consider that
    # updates may contains updates of the same tag
    def update(self, updates):
        if not isinstance(updates, list):
            updates = list(updates)

        if not updates:
            return READY_ACK_FUTURE

        msg = cloud_tags_pb2.TClientMessage()
        items = msg.Update.Items
        for update in updates:
            self._form_update_item(items.add(), update)

        return self._push(msg)

    def serial_update(self, update):
        msg = cloud_tags_pb2.TClientMessage()
        self._form_update_item(msg.SyncedUpdate.Data, update)
        return self._push(msg)

    def _do_stop(self, value):
        self._should_stop = value
        #self._should_stop_cond.notify_all()
        if self._should_and_can_stop():
            self._should_and_can_stop_cond.notify_all()
        self._outgoing_not_empty.notify()

    def is_stopped(self):
        return self._stopped

    def _stop(self, wait=True, timeout=None):
        if self._stopped:
            return

        new_value = self._ST_WAIT if wait else self._ST_NOWAIT

        with self._lock:
            if self._should_stop < new_value:
                logging.info("Stopping YtTags.Client (%s)" % '_ST_WAIT' if wait == self._ST_WAIT else '_ST_NOWAIT')
                self._do_stop(new_value)

            elif self._should_stop > new_value:
                logging.warning("stop() called with lower stop-level")

        self._connect_thread.join(timeout) # TODO sleeps

    def stop(self, timeout=None):
        self._stop(timeout=timeout) # TODO Don't wait at all, if we disconnected for a long time
        self._stop(wait=False)

        # TODO Kosher
        if not self._connect_thread.is_alive():
            self._stopped = True

    def _should_and_can_stop(self):
        return self._should_stop == self._ST_NOWAIT \
            or self._should_stop == self._ST_WAIT \
                and not self._outgoing \
                and not self._running

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.stop()

    def _push(self, msg, code=None):
        promise = Promise()

        with self._lock:
            if self._should_stop != self._ST_NONE:
                promise.set(exc=ServiceStopped())
            else:
                self._do_push(msg, promise, code)

        return promise.to_future()

    def _do_push(self, msg, promise, code=None, is_resend=False):
        msg_id = self._next_message_id
        self._next_message_id += 1

        msg.Id = msg_id

        task = self._Task(promise, msg, is_resend)

        if code:
            code()

        self._running[msg_id] = task
        self._outgoing.append(task)

        self._outgoing_not_empty.notify()

    def _write_loop(self):
        self._io_loop("Output", self._write_loop_impl, socket.SHUT_WR)

    def _read_loop(self):
        def after_shutdown():
            with self._lock:
                self._io._read_finished = True
                self._outgoing_not_empty.notify()
        self._io_loop("Input", self._read_loop_impl, socket.SHUT_RD, after_shutdown)

    def _io_loop(self, type_str, loop, how, after_shutdown=None):
        failed = True
        try:
            loop()
            failed = False
        except Exception as e:
            logging.exception("%s error" % type_str)

        #logging.debug("%s io thread stopped" % type_str)

        try:
            self._io._connection.shutdown(how)
        except socket.error as e:
            if e.errno != errno.ENOTCONN:
                logging.warning("Error on socket shutdown(%d): %s" % (how, e))

        if after_shutdown:
            after_shutdown()

        with self._lock:
            self._io._thread_count -= 1

            if not self._io._thread_count:
                self._io._connected = False
                #self._connection_state_changed.notify_all()

    def _write_loop_impl(self):
        outgoing = self._outgoing
        conn = self._io._connection

        def need_emergency_stop():
                # FIXME _read_finished is enough?
            return self._io._read_finished \
                or self._io._bye_received \
                or self._should_stop == self._ST_NOWAIT

        while True:
            with self._lock:
                while not(need_emergency_stop() or self._should_stop == self._ST_WAIT or outgoing):
                    self._outgoing_not_empty.wait()

                if need_emergency_stop() or self._should_stop == self._ST_WAIT and not outgoing:
                    break

                assert bool(outgoing)

                msg = outgoing.popleft().msg

                if not outgoing:
                    self._outgoing_empty.notify_all()

            #logging.debug("send message to server %s" % msg)
            conn.send(msg)

    # TODO Rewrite server.cpp
        if need_emergency_stop():
            return
        with self._lock:
            while not self._should_and_can_stop():
                self._should_and_can_stop_cond.wait()

    def wait_outgoing_empty(self):
        with self._lock:
            if not self._outgoing:
                return
            self._outgoing_empty.wait()

    def _read_loop_impl(self):
        conn = self._io._connection

        while True:
            msg = conn.recv()

            if msg is None: # EOF
                break

            self._process_server_message(msg)

    class _ServerMessage(object):
        class Event_(object):
            def __init__(self, msg):
                self.tag_name = msg.TagName
                self.event = msg.TagEvent
                self.version = msg.TagVersion
                self.last_reset_version = msg.LastResetVersion
                self.last_reset_comment = msg.Comment # TODO XXX This code lies

            def __repr__(self):
                state = self.__dict__.copy()
                state['event'] = TagEventName[state['event']]
                return '<Event(' + asjsonstring(state) + ')>'

        @classmethod
        def Event(cls, msg):
            return [cls.Event_(item) for item in msg.Event.Items]

        class Lookup_(object):
            def __init__(self, msg):
                self.tag_name = msg.TagName
                self.is_set = msg.IsSet_
                self.version = msg.TagVersion
                self.last_reset_version = msg.LastResetVersion
                self.last_reset_comment = msg.LastResetComment

            def __repr__(self):
                return '<Lookup(' + asjsonstring(self.__dict__) + ')>'

        @classmethod
        def Lookup(cls, msg):
            return {item.TagName: cls.Lookup_(item) for item in msg.Items}

        @classmethod
        def Match(cls, msg):
            if msg.Error:
                return (None, RuntimeError(msg.Error))
            return ([cls.Lookup_(item) for item in msg.Items], None)

        @classmethod
        def Subscriptions(cls, msg):
            return msg.Tags

    def _process_server_message(self, msg):
        # WhichOneof doesn't work
        # https://github.com/google/protobuf/commit/0971bb0d57aa6f2db1abee4008b365d52b402891
        # type = msg.WhichOneof('Data')

        def first(pred, iterable):
            for item in iterable:
                if pred(item):
                    return item

        type = first(msg.HasField, ['Event', 'Bye', 'Ack', 'Lookup', 'Subscriptions', 'Match'])

        if type is None:
    # XXX Client will looped in this error
            raise NotImplementedError("Unknown server message type for [%s]" % msg)

        if type == 'Event':
            for ev in self._ServerMessage.Event(msg):
                try:
                    self._on_event(ev)
                except:
                    logging.exception("Failed to process journal event")
            return

        elif type == 'Bye':
            #logging.debug("...")
            with self._lock:
                self._io._bye_received = True
                self._outgoing_not_empty.notify()
            return

        data = getattr(msg, type)

        if not data.HasField('RequestId'):
            raise RuntimeError('No .RequestId')
        request_id = data.RequestId

        with self._lock:
            promise = self._running.pop(request_id).promise # FIXME , None and raise
            #if not self._running:
                #self._running_empty.notify_all()
            if self._should_and_can_stop():
                self._should_and_can_stop_cond.notify_all()

        if type == 'Ack':
            promise.set(None)
        elif type == 'Lookup':
            promise.set(self._ServerMessage.Lookup(msg.Lookup))
        elif type == 'Match':
            promise.set(*self._ServerMessage.Match(msg.Match))
        elif type == 'Subscriptions':
            promise.set(self._ServerMessage.Subscriptions(msg.Subscriptions))
        else:
            assert False


# 1. 
#   RPC -> Reconnectable(client.py)
#   packet -> SUCCESSFULL/Reset -> { journal(намерение), some_backupable_state } -> Reconnectable(client.py)
#   <internals> -> subscribe

# 2. Надо ли уметь стопить REM так, чтобы он при недоступном облаке тегов стопился сразу?

# myself:
#   1. self._raise_on_push = True
#   2. self._push(TBye(), dont_raise=True)
#   3. if .pop() is TBye():
#           break && shutdown(socket.SHUT_WR)
#   4. msg = read()
#        if msg is TBye():
#           break && shutdown(socket.SHUT_RD)

# from server:
#   1. self._raise_on_push = True
#   2. self._push(TBye(), dont_raise=True)
#   3. if .pop() is TBye():
#           break && shutdown(socket.SHUT_WR)
#   4. msg = read()
#        if msg is TBye():
#           break && shutdown(socket.SHUT_RD)

# если сервер просто закроет на чтение, я могу словить SIGPIPE

#def SingleAddressConnection(host, port):
    #return lambda : ProtobufConnection(host, port)

