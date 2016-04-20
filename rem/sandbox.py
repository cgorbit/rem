import sys
import requests
import json


class TaskPriority(object):
    class Class(object):
        BACKGROUND = 'BACKGROUND'
        SERVICE    = 'SERVICE'
        USER       = 'USER'

    class SubClass(object):
        LOW    = 'LOW'
        NORMAL = 'NORMAL'
        HIGH   = 'HIGH'


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

    #def update(self, *args, **kwargs):
        #if args:
            #if kwargs or len(args) > 1 or not isinstance(args[0], dict):
                #raise ValueError("update({...}) or update(k1=v1, k2=v2,...)")
            #updates = args[0]
        #else:
            #updates = kwargs
        #new = self.dict
        #new.update(updates)
        #ret = self._make_call('PUT', self.BASE_URL + '/%s' % self.id, new)
        #self.__dict__.update(ret)
        #return self

    def _update(self):
        ret = self._make_call('GET', self.BASE_URL + '/%s' % self.id)
        self.__dict__.update(ret)
        return self

    def dumps(self, indent=3):
        return json.dumps(self.dict, indent=indent, ensure_ascii=False)

    def dump(self, indent=3):
        print >>sys.stderr, self.dumps(indent=indent)


class Sandbox(object):
    def __init__(self, url, oauth_token, debug=False):
        self.url = url
        self.oauth_token = oauth_token
        self.debug = debug

    def _make_call(self, method, path, data=None, raw_result=False, succ_code=200):
        if self.debug:
            print >>sys.stderr, '+', method, path, json.dumps(data)

        headers = {
            'Authorization': 'OAuth ' + self.oauth_token,
            'Content-Type': 'application/json',
        }
        r = requests.request(
            method,
            self.url + path,
            data=json.dumps(data),
            headers=headers,
            verify=False
        )

        if self.debug:
            print >>sys.stderr, '+ response from', path, r.status_code, r.text.encode('utf-8')

        if r.status_code != succ_code:
            raise RuntimeError(r.text)
            #try:
                #answer = r.json()
            #except:
                #msg = "Can't get 'msg' from answer"
                #code = None
            #else:
                #msg = answer['msg']
                #code = answer['code']

            #if code == 'CONFLICT_STATE':
                #excls = TolokaApi.ConflictState
            #else:
                #excls = TolokaApi.Exception

            #raise excls("Not 200 Ok answer from api: %s, %s, %s" % (r.status_code, msg, r.text.encode('utf-8')))

        return r.text if raw_result else r.json()

    class TaskProxy(_ProxyObject):
        BASE_URL = 'task'

        def update(self, priority=None, owner=None, notifications=None):
            params = {
            }
            if priority is not None:
                params['priority'] = {'class': priority[0], 'subclass': priority[1]}
            if owner is not None:
                params['owner'] = owner
            if notifications is not None:
                params['notifications'] = notifications

            if params:
                self._make_call('PUT', 'task/%d' % self.id, params, succ_code=204, raw_result=True)

            self._update()

        def start(self):
            res = self._make_call(
                'PUT',
                'batch/tasks/start',
                [self.id],
            )[0]

            if res['status'] == 'ERROR':
                raise RuntimeError(res['message'])

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

    def list_task_statuses(self, ids):
        #url = '/task?limit=%d&' % len(ids)
        #url += '&'.join('id=%d' % id for id in ids)

        url = '/task?limit=%d&id=%s' % (len(ids), ','.join(map(str, ids)))

        resp = self._make_call('GET', url)
        return {t['id']: t['status'] for t in resp['items']}

    def upload_resource(self, type, name, protocol, remote_file_name, ttl=None, arch=None, **kwargs):
        context = {
            'created_resource_name': name,
            'remote_file_protocol': protocol,
            'remote_file_name': remote_file_name,
            'resource_type': 'CLOUD_TAGS_NIRVANA_BERNSTEIN_BINARY',
        }

        if ttl is not None:
            context['resource_attrs'] = 'ttl=%d' % ttl

        if arch is not None:
            context['resource_arch'] = arch

        return self.create_task('REMOTE_COPY_RESOURCE', context, **kwargs)
