import time
import threading

from rem.profile import ProfiledThread
from rem_logging import logger as logging
from rem.future import Promise
import rem.delayed_executor as delayed_executor


class _Resource(object):
    def __init__(self, resource_type):
        self.promise = Promise()
        self.resolve_time = None
        self.resource_type = resource_type


class SandboxReleasesResolver(object):
    _RETRY_INTERVAL = 15.0 # TODO More complicated policy

    def __init__(self, action_queue, client):
        self._action_queue = action_queue
        self._client = client

        self._lock = threading.Lock()
        self._releases = {}

    def resolve(self, resource_type):
        with self._lock:
            rel = self._releases.get(resource_type)
            if rel:
                if rel.resolve_time is None:
                    return rel.promise.to_future()

                if time.time() - rel.resolve_time < self._RESOLVE_RESULT_TTL:
                    return rel.promise.to_future()

                self._releases.pop(resource_type)

            rel = _Resource(resource_type)
            self._releases[resource_type] = rel

        self._resolve(rel)

        return rel.promise.to_future()

    def _resolve(self, rel):
        self._action_queue.invoke(lambda : self._do_resolve(rel))

    def _do_resolve(self, rel):
        try:
            self._do_do_resolve(rel)
# TODO Permanent errors?
        except Exception as e:
            logging.warning('Failed to list_latest_releases for %s: %s' % (rel.resource_type, e))
            logging.exception('Failed to list_latest_releases for %s' % rel.resource_type)
            delayed_executor.schedule(lambda : self._resolve(rel), timeout=self._RETRY_INTERVAL)

# XXX FIXME BROKEN, DELETED
# FIXME Move logic to combined rem.sandbox?
    def _do_do_resolve(self, rel):
        resource_type = rel.resource_type

        releases = self._client.list_latest_releases(resource_type, limit=1)

        if not releases['items']:
            rel.promise.set(None, RuntimeError("No such release type %s" % resource_type))
            return

        task_id = releases['items'][0]['task_id']

        release = self._client.get_release(task_id)

        resources = [
            res['resource_id'] for res in release['resources']
                if res['type'] == resource_type
        ]

        if not resources:
            raise RuntimeError("No resources of type %s in release %d" % (resource_type, task_id))

        rel.promise.set(resources[0])

