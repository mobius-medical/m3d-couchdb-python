import requests.exceptions
from requests_toolbelt import sessions
import six

from couchdb import exceptions


class Session(object):

    def __init__(self, base_url):
        self._base_session = sessions.BaseUrlSession(base_url=base_url)

    @property
    def base_url(self):
        return self._base_session.base_url

    @base_url.setter
    def base_url(self, url):
        self._base_session.base_url = url

    def request(self, method, url, *args, **kwargs):
        resp = self._base_session.request(method, str(url), *args, **kwargs)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            six.raise_from(exceptions.http_error_lookup(exc.response.status_code, exc.response.reason), exc)
        except requests.exceptions.Timeout as exc:
            six.raise_from(exceptions.Timeout, exc)
        except requests.exceptions.RequestException as exc:
            six.raise_from(exceptions.RequestsException, exc)
        return resp

    def head(self, url, **kwargs):
        return self.request("HEAD", url=url, **kwargs)

    def get(self, url, **kwargs):
        return self.request("GET", url=url, **kwargs)

    def put(self, url, data=None, **kwargs):
        return self.request("PUT", data=data, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        return self.request("POST", url, data=data, json=json, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url, **kwargs)
