import requests

class Nanny(object):
    def __init__(self, host, token):
        self._host = host
        self._token = token

    def list_instances(self, srv):
        return [
            (instance['hostname'].encode('ascii'), int(instance['port']))
                for instance in self._request('%s/current_state/instances/' % srv)
        ]

    def _request(self, path):
        headers = {
            'Authorization': 'OAuth ' + self._token,
            'Content-Type': 'application/json',
        }

        r = requests.request(
            'GET',
            'http://' + self._host + '/v2/services/' + path,
            #data=json.dumps(data),
            headers=headers,
            verify=False
        )

        return r.json()['result']

