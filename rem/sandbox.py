import sys
import requests
import json
import requests.exceptions as exceptions


class TaskPriority(object):
    class Class(object):
        BACKGROUND = 'BACKGROUND'
        SERVICE    = 'SERVICE'
        USER       = 'USER'

        _ALL = set([BACKGROUND, SERVICE, USER])

        @classmethod
        def from_string(cls, s):
            if s not in cls._ALL:
                raise ValueError("Unknown priority class '%s'" % s)
            return s

    class SubClass(object):
        LOW    = 'LOW'
        NORMAL = 'NORMAL'
        HIGH   = 'HIGH'

        _ALL = set([LOW, NORMAL, HIGH])

        @classmethod
        def from_string(cls, s):
            if s not in cls._ALL:
                raise ValueError("Unknown priority subclass '%s'" % s)
            return s

    @classmethod
    def from_string(cls, s):
        prio = s.upper().split(':')

        if len(prio) != 2:
            raise ValueError("Malformed task priority '%s'" % s)

        return (cls.Class.from_string(prio[0]),
                cls.SubClass.from_string(prio[1]))


class NetworkError(RuntimeError):
    pass

class ServerInternalError(RuntimeError):
    pass

class UserError(RuntimeError):
    pass


_ERROR_BY_CODE = [
    None,
    RuntimeError,
    RuntimeError,
    RuntimeError,
    UserError,
    ServerInternalError,
]


class _ProxyObject(object):
    def __init__(self, api, config):
        self.__dict__.update(config)
        self._api = api

    def _make_call(self, *args, **kwargs):
        return self._api._make_call(*args, **kwargs)

    @property
    def dict(self):
        ret = dict(self.__dict__)
        del ret['_api']
        return ret

    def _update(self):
        ret = self._make_call('GET', self.BASE_URL + '/%s' % self.id)
        self.__dict__.update(ret)
        return self

    def dumps(self, indent=3):
        return json.dumps(self.dict, indent=indent, ensure_ascii=False)


class Client(object):
    def __init__(self, url, oauth_token=None, debug=False, timeout=None):
        self.url = url
        self.oauth_token = oauth_token
        self.debug = debug
        self.timeout = timeout

    def _make_call(self, method, path, data=None, raw_result=False, succ_code=200):
        if self.debug:
            print >>sys.stderr, '+', method, path, json.dumps(data)

        headers = {
            'Content-Type': 'application/json',
        }
        if self.oauth_token:
            headers['Authorization'] = 'OAuth ' + self.oauth_token

        try:
            r = requests.request(
                method,
                self.url + path,
                data=json.dumps(data),
                headers=headers,
                verify=False,
                timeout=self.timeout
            )
        except (exceptions.Timeout, exceptions.SSLError, exceptions.ConnectionError) as e:
            raise NetworkError(e)

        if self.debug:
            print >>sys.stderr, '+ response from', path, r.status_code, r.text.encode('utf-8')

        if r.status_code != succ_code:
            # TODO use r.json()['message']
            raise _ERROR_BY_CODE[r.status_code / 100](r.text)

        return r.text if raw_result else r.json()

    class TaskProxy(_ProxyObject):
        BASE_URL = 'task'

        def update(self, priority=None, owner=None, notifications=None, max_restarts=None,
                         kill_timeout=None, description=None):
            params = {
            }
            if priority is not None:
                params['priority'] = {'class': priority[0], 'subclass': priority[1]}
            if owner is not None:
                params['owner'] = owner
            if notifications is not None:
                params['notifications'] = notifications
            if max_restarts is not None:
                params['max_restarts'] = max_restarts
            if kill_timeout is not None:
                params['kill_timeout'] = kill_timeout
            if description is not None:
                params['description'] = description

            if params:
                self._make_call('PUT', 'task/%d' % self.id, params, succ_code=204, raw_result=True)

            self._update()

        def start(self):
            self._start(self._api, self.id)

        def get_context(self):
            return self._api._make_call('GET', 'task/%d/context' % self.id)

            #if res['status'] == 'ERROR':
                #raise RuntimeError(res['message'])

        @staticmethod
        def _start(api, id):
            res = api._make_call(
                'PUT',
                'batch/tasks/start',
                [id],
            )[0]

            if res['status'] == 'ERROR':
                raise RuntimeError(res['message'])

    def start_task(self, id):
        self.TaskProxy._start(self, id)

    def Task(self, id):
        return self.TaskProxy(self, self._make_call('GET', '/task/%d' % id))

    def create_task(self, type, context, **kwargs):
        params = {
            'type': type,
            'context': context
        }
        task = self.TaskProxy(self, self._make_call('POST', '/task', params, succ_code=201))
        if kwargs:
            task.update(**kwargs)
        return task

    def list_task_resources(self, id):
        return self._make_call('GET', '/task/%d/resources' % id)

    def list_task_statuses(self, ids):
        url = '/task?limit=%d&id=%s' % (len(ids), ','.join(map(str, ids)))
        resp = self._make_call('GET', url)
        return {t['id']: t['status'] for t in resp['items']}

    def create_resource_upload_task(self, type, name, protocol, remote_file_name, ttl=None, arch=None, **kwargs):
        context = {
            'created_resource_name': name,
            'remote_file_protocol': protocol,
            'remote_file_name': remote_file_name,
            'resource_type': type,
        }

        if ttl is not None:
            context['resource_attrs'] = 'ttl=%d' % ttl

        if arch is not None:
            context['resource_arch'] = arch

        return self.create_task('REMOTE_COPY_RESOURCE', context, **kwargs)
