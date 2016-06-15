import time
import threading
import re
from collections import namedtuple

from rem_logging import logger as logging
from rem.future import Promise
import rem.delayed_executor as delayed_executor


_RESOURCE_DESCR_RE = re.compile('^(?:(re[sl]):)?([a-zA-Z_]\w*)(?::owner=(\w+))?$')


class _Resource(object):
    def __init__(self, request):
        self.promise = Promise()
        self.resolve_time = None
        self.request = request


class MalformedResourceDescr(ValueError):
    pass


class SandboxReleasesResolver(object):
    _RETRY_INTERVAL = 15.0 # TODO More complicated policy
    _RESOLVE_RESULT_TTL = 5.0 # FIXME

    # XXX Must be hashable type
    Request = namedtuple('Request', ['type', 'owner', 'released'])

    def __init__(self, action_queue, client):
        self._action_queue = action_queue
        self._client = client

        self._lock = threading.Lock()
        self._releases = {}

    @classmethod
    def _parse_resource_descr(cls, descr):
        m = _RESOURCE_DESCR_RE.match(descr)
        if not m:
            raise MalformedResourceDescr()

        groups = m.groups()

        return cls.Request(
            type=groups[1],
            owner=groups[2],
            released=groups[0] is None or groups[0] == 'rel'
        )

    def resolve(self, descr):
        req = self._parse_resource_descr(descr)
        logging.debug(str(req))

        #import traceback
        #logging.debug(''.join(traceback.format_stack()))

        with self._lock:
            rel = self._releases.get(req)
            if rel:
                if rel.resolve_time is None:
                    return rel.promise.to_future()

                if time.time() - rel.resolve_time < self._RESOLVE_RESULT_TTL:
                    return rel.promise.to_future()

                self._releases.pop(req)

            rel = _Resource(req)
            self._releases[req] = rel

        self._resolve(rel)

        return rel.promise.to_future()

    def _resolve(self, rel):
        self._action_queue.invoke(lambda : self._do_resolve(rel))

    def _do_resolve(self, rel):
        try:
            self._do_do_resolve(rel)
        # TODO Fail on permanent errors
        except Exception as e:
            #logging.warning('Failed to list_latest_releases for %s: %s' % (rel.request, e))
            logging.exception('Failed to list_latest_releases for %s' % rel.request)
            delayed_executor.schedule(lambda : self._resolve(rel), timeout=self._RETRY_INTERVAL)

    # FIXME Move 2-step logic to rem.sandbox?
    def _do_do_resolve(self, rel):
        request = rel.request

        resource = self._client.get_latest_resource(
            type=request.type,
            owner=request.owner,
            released=request.released
        )

        rel.resolve_time = time.time()

        if not resource:
            logging.warning("Failed to resolve %s" % request)
            rel.promise.set(None, RuntimeError("Can't find resource %s" % request))
            return

        logging.debug("%s resolved to %s" % (request, resource))
        rel.promise.set(resource['id'])

