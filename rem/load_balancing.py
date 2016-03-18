import time
import logging
import random
from collections import deque
from rem_logging import logger as logging

class _InstanceGroup(object):
    class _Instance(object):
        FAILS_HISTORY_LENGTH = 3

        def __init__(self, addr):
            self.addr = addr
            self._fails = deque()
            self._last_cant_connect = 0

        def __repr__(self):
            return '<_Instance %s; fails=%s; lcc=%s>' % (self.addr, self._fails, self._last_cant_connect)

        def register_connect(self):
            self._last_cant_connect = 0

        def register_disconnect(self):
            logging.debug("++ register_disconnect(%s)" % (self.addr,))

            self._fails.append(time.time())

            if len(self._fails) > self.FAILS_HISTORY_LENGTH:
                self._fails.popleft()

        def register_cant_connect(self, error):
            logging.debug("++ register_cant_connect(%s, %s)" % (self.addr, error))
            #(void)error
            self._last_cant_connect = time.time()

    # XXX TODO XXX TODO XXX
        def is_good(self):
            if time.time() - self._last_cant_connect < 1.0:
                return False

            fails = self._fails

            def pairs():
                cont = fails
                return [(cont[idx - 1], cont[idx]) for idx, _ in enumerate(cont) if idx]

            if len(fails) == self.FAILS_HISTORY_LENGTH:
                always_fail = all(v1 - v0 < 10 for v0, v1 in pairs())
                if always_fail and time.time() - fails[-1] < 15:
                    return False

            return True

    def __init__(self, addrs):
        self._instances = deque(self._Instance(addr) for addr in addrs)

    def __repr__(self):
        sep = '\n    '
        return '<_InstanceGroup [' + sep + sep.join(repr(i) for i in self._instances) + '] >'

    def update_instances(self, new_addrs):
        new_addrs = set(new_addrs)

        old_addrs = set(i.addr for i in self._instances)

        if old_addrs == new_addrs:
            return

        to_add = new_addrs - old_addrs

        self._instances = deque(
            [self._Instance(addr) for addr in to_add] + \
            [i for i in self._instances if i.addr in new_addrs]
        )

    def get(self):
        for idx, instance in enumerate(self._instances):
            if instance.is_good():
                if len(self._instances) > 1:
                    self._instances.rotate(-(idx + 1))
                return instance


class PlainInstancesList(object):
    def __init__(self, list_instances, list_update_interval=600):
        self._list_instances = list_instances
        self._last_list_update_time = 0
        self._list_update_interval = list_update_interval

# XXX TODO XXX TODO XXX TODO REMOVE
        self._list_update_interval = 10

        self._instances_group = None

    def _try_update_addrs(self):
        try:
            new_instances = self._list_instances()
        except Exception as e:
            logging.exception("Failed to get addrs")
            return

        random.shuffle(new_instances)

        logging.debug('_list_instances => %s;' % new_instances)

        if not new_instances:
            logging.error("Empty addrs list")
            return

        if not self._instances_group:
            self._instances_group = _InstanceGroup(new_instances)
        else:
            self._instances_group.update_instances(new_instances)

        self._last_list_update_time = time.time()

        return True

    def __call__(self):
        if not self._instances_group:
            if not self._try_update_addrs():
                return

        if time.time() - self._last_list_update_time > self._list_update_interval:
            self._try_update_addrs()

        instance = self._instances_group.get()
        logging.debug('PlainInstancesList() -> %s' % instance)
        return instance


class LocalAndRemoteInstances(object):
    _LOCAL_GROUP_MAX_WAIT_TIME = 60.0

    def __init__(self, list_addrs, list_update_interval=600):
        self._list_addrs = list_addrs
        self._last_list_update_time = 0
        self._list_update_interval = list_update_interval
        self._list_update_interval = 10 # TODO REMOVE
        self._local_group = None
        self._local_group_first_fail_time = None
        self._remote_group = None

    def _try_update_addrs(self):
        try:
            local, remote = self._list_addrs()
        except Exception as e:
            logging.exception("Failed to get addrs")
            return

        random.shuffle(local)
        random.shuffle(remote)

        logging.debug('_list_addrs => local = %s; remote = %s' % (local, remote))

        if not local and not remote:
            logging.error("Empty addrs list")
            return

        if not local:
            logging.warning("No local addrs: %s" % ((local, remote),))
            return

        if not self._local_group:
            self._local_group = _InstanceGroup(local)
            self._remote_group = _InstanceGroup(remote)
        else:
            self._local_group.update_instances(local)
            self._remote_group.update_instances(remote)

        self._last_list_update_time = time.time()

        return True

    def __call__(self):
        if not self._local_group:
            if not self._try_update_addrs():
                return

        if time.time() - self._last_list_update_time > self._list_update_interval:
            self._try_update_addrs()

        instance = self._local_group.get()
        if instance:
            return instance

        logging.debug('local_group give no instance')

        if not self._local_group_first_fail_time:
            self._local_group_first_fail_time = time.time()

        if time.time() - self._local_group_first_fail_time < self._LOCAL_GROUP_MAX_WAIT_TIME:
            return

        instance = self._remote_group.get()
        if not instance:
            logging.debug('remote_group give no instance')

        return instance


class ConnectionFromInstances(object):
    def __init__(self, get_instance, connect):
        self._get_instance = get_instance
        self._connect = connect
        self._current_instance = None

    def __call__(self):
        if self._current_instance:
            self._current_instance.register_disconnect()
            self._current_instance = None

        while True:
            instance = self._get_instance()
            logging.debug('ConnectionFromInstances instance = %s' % instance) # TODO REMOVE

            if not instance:
                return

            try:
                connection = self._connect(instance.addr)
            except Exception as e:
                instance.register_cant_connect(e)
            else:
                instance.register_connect()
                break

        self._current_instance = instance
        return connection

