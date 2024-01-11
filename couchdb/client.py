# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2009 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Python client API for CouchDB.

>>> server = Server()
>>> db = server.create('python-tests')
>>> doc_id, doc_rev = db.save({'type': 'Person', 'name': 'John Doe'})
>>> doc = db[doc_id]
>>> doc['type']
u'Person'
>>> doc['name']
u'John Doe'
>>> del db[doc.id]
>>> doc.id in db
False

>>> del server['python-tests']
"""
from __future__ import division  # For Python 2.x.

import io
import json
import itertools
import mimetypes
import os
from types import FunctionType
from inspect import getsource
from textwrap import dedent

import furl
import six
import requests.exceptions
from requests_toolbelt import sessions

from couchdb import http, util, exceptions, views

__all__ = ['Server', 'Database', 'Document', 'ViewResults', 'Row']
__docformat__ = 'restructuredtext en'


DEFAULT_BASE_URL = os.environ.get('COUCHDB_URL', 'http://localhost:5984/')
BIN_MIME = "application/octet-stream"


def _jsons(data, indent=None):
    """Convert data into JSON string."""
    return json.dumps(data, ensure_ascii=False, indent=indent)


def quote(name, safe=''):
    return furl.quote(name, safe=safe)


class Session(object):
    """Wrapper around BaseUrlSession that automatically wraps certain exceptions when making requests"""

    def __init__(self, base_url=None):
        self._base_session = sessions.BaseUrlSession(base_url=base_url)

    @property
    def base_url(self):
        return self._base_session.base_url

    @base_url.setter
    def base_url(self, url):
        self._base_session.base_url = url

    def request(self, method, url, *args, **kwargs):
        try:
            resp = self._base_session.request(method, str(url), *args, **kwargs)
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
        return self.request("PUT", url=url, data=data, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        return self.request("POST", url=url, data=data, json=json, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("DELETE", url=url, **kwargs)


class Server(object):
    """Representation of a CouchDB server.

    >>> server = Server() # connects to the local_server
    >>> remote_server = Server('http://example.com:5984/')
    >>> secure_remote_server = Server('https://username:password@example.com:5984/')

    This class behaves like a dictionary of databases. For example, to get a
    list of database names on the server, you can simply iterate over the
    server object.

    New databases can be created using the `create` method:

    >>> db = server.create('python-tests')
    >>> db
    <Database 'python-tests'>

    You can access existing databases using item access, specifying the database
    name as the key:

    >>> db = server['python-tests']
    >>> db.name
    'python-tests'

    Databases can be deleted using a ``del`` statement:

    >>> del server['python-tests']
    """

    def __init__(self, url=DEFAULT_BASE_URL, session=None):
        """Initialize the server object.

        :param url: the URI of the server (for example ``http://localhost:5984/``)
        """
        self._url = url
        if session:
            self._session = session
            self._session.base_url = url
        else:
            self._session = Session(base_url=self._url)
        self._version_info = None

    @property
    def url(self):
        return self._url

    @property
    def session(self):
        return self._session

    def __contains__(self, name):
        """Return whether the server contains a database with the specified
        name.

        :param name: the database name
        :return: `True` if a database with the name exists, `False` otherwise
        """
        try:
            self._session.head(quote(name))
            return True
        except exceptions.HTTPNotFound:
            return False

    def __iter__(self):
        """Iterate over the names of all databases."""
        return iter(self._session.get('_all_dbs').json())

    def __len__(self):
        """Return the number of databases."""
        return len(self._session.get('_all_dbs').json())

    def __nonzero__(self):
        """Return whether the server is available."""
        try:
            self._session.head("/")
            return True
        except exceptions.RequestsException:
            return False

    def __bool__(self):
        return self.__nonzero__()

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.url)

    def __delitem__(self, name):
        """Remove the database with the specified name.

        :param name: the name of the database
        :raise MissingDatabase: if no database with that name exists
        """
        try:
            self._session.delete(quote(name))
        except exceptions.HTTPNotFound as exc:
            six.raise_from(exceptions.MissingDatabase("Database does not exist"), exc)

    def __getitem__(self, name):
        """Return a `Database` object representing the database with the
        specified name.

        :param name: the name of the database
        :return: a `Database` object representing the database
        :rtype: `Database`
        :raise ResourceNotFound: if no database with that name exists
        """
        return Database(self, name, check=True)

    def config(self, node="_local"):
        """The configuration of the CouchDB server.

        The configuration is represented as a nested dictionary of sections and
        options from the configuration files of the server, or the default
        values for options that are not explicitly configured.

        :rtype: `dict`
        """
        path = furl.Path(['_node', node, '_config'])
        return self._session.get(str(path)).json()

    def version(self):
        """The version string of the CouchDB server.

        Note that this results in a request being made, and can also be used
        to check for the availability of the server.

        :rtype: `unicode`"""
        return self._session.get("/").json()['version']

    def version_info(self):
        """The version of the CouchDB server as a tuple of ints.

        Note that this results in a request being made only at the first call.
        Afterwards the result will be cached.

        :rtype: `tuple(int, int, int)`"""
        if self._version_info is None:
            version = self.version()
            self._version_info = tuple(map(int, version.split('.')))
        return self._version_info

    def stats(self, name=None, node="_local"):
        """Server statistics.

        :param name: name of single statistic, e.g. httpd/requests
                     (None -- return all statistics)
        :param node: node for which to return statistics
        """
        path = furl.Path(['_node', node, '_stats'])
        if name:
            path.add(name)
        return self._session.get(str(path)).json()

    def tasks(self):
        """A list of tasks currently active on the server."""
        return self._session.get("_active_tasks").json()

    def uuids(self, count=1):
        """Retrieve a batch of uuids

        :param count: a number of uuids to fetch
        :return: a list of uuids
        """
        data = self._session.get("_uuids", params={'count': count}).json()
        return data['uuids']

    def create(self, name):
        """Create a new database with the given name.

        :param name: the name of the database
        :return: a `Database` object representing the created database
        :rtype: `Database`
        :raise DatabaseExists: if a database with that name already exists
        """
        try:
            self._session.put(quote(name))
        except exceptions.HTTPPreconditionFailed as exc:
            six.raise_from(exceptions.DatabaseExists("Database already exists"), exc)
        return self[name]

    def delete(self, name):
        """Delete the database with the specified name.

        :param name: the name of the database
        :raise ResourceNotFound: if a database with that name does not exist
        :since: 0.6
        """
        del self[name]

    def replicate(self, source, target, **options):
        """Replicate changes from the source database to the target database.

        :param source: URL of the source database
        :param target: URL of the target database
        :param options: optional replication args, e.g. continuous=True
        """
        # CouchDB requires full URLs for source and target even on the same server,
        # if we don't get a netloc we assume it's only a database name
        if not furl.furl(source).netloc:
            source = furl.furl(self.url).set(path=[source]).url
        if not furl.furl(target).netloc:
            target = furl.furl(self.url).set(path=[target]).url
        data = {'source': source, 'target': target}
        data.update(options)
        return self._session.post("_replicate", json=data).json()

    def add_user(self, name, password, roles=None):
        """Add regular user in authentication database.

        :param name: name of regular user, normally user id
        :param password: password of regular user
        :param roles: roles of regular user
        :return: (id, rev) tuple of the registered user
        :rtype: `tuple`
        """
        try:
            user_db = self['_users']
        except exceptions.MissingDatabase:
            user_db = self.create('_users')
        return user_db.save({
            '_id': 'org.couchdb.user:' + name,
            'name': name,
            'password': password,
            'roles': roles or [],
            'type': 'user',
        })

    def remove_user(self, name):
        """Remove regular user in authentication database.

        :param name: name of regular user, normally user id
        """
        user_db = self['_users']
        doc_id = 'org.couchdb.user:' + name
        del user_db[doc_id]

    def login(self, name, password):
        """Login regular user in couch db

        :param name: name of regular user, normally user id
        :param password: password of regular user
        """
        try:
            return self._session.post("_session", json={'name': name, 'password': password})
        except exceptions.HTTPForbidden as exc:
            six.raise_from(exceptions.LoginFailed, exc)

    def logout(self):
        """Logout regular user in couch db

        :param token: token of login user
        :return: True if successfully logout
        :rtype: bool
        """
        return self._session.delete("_session")

    def get_token(self):
        """Get user token

        """
        return self._session.get("_session")


class Database(object):
    """Representation of a database on a CouchDB server.

    >>> server = Server()
    >>> db = server.create('python-tests')

    New documents can be added to the database using the `save()` method:

    >>> doc_id, doc_rev = db.save({'type': 'Person', 'name': 'John Doe'})

    This class provides a dictionary-like interface to databases: documents are
    retrieved by their ID using item access

    >>> doc = db[doc_id]
    >>> doc                 #doctest: +ELLIPSIS
    <Document u'...'@... {...}>

    Documents are represented as instances of the `Row` class, which is
    basically just a normal dictionary with the additional attributes ``id`` and
    ``rev``:

    >>> doc.id, doc.rev     #doctest: +ELLIPSIS
    (u'...', ...)
    >>> doc['type']
    u'Person'
    >>> doc['name']
    u'John Doe'

    To update an existing document, you use item access, too:

    >>> doc['name'] = 'Mary Jane'
    >>> db[doc.id] = doc

    The `save()` method creates a document with a random ID generated by
    CouchDB (which is not recommended). If you want to explicitly specify the
    ID, you'd use item access just as with updating:

    >>> db['JohnDoe'] = {'type': 'person', 'name': 'John Doe'}

    >>> 'JohnDoe' in db
    True
    >>> len(db)
    2

    >>> del server['python-tests']

    If you need to connect to a database with an unverified or self-signed SSL
    certificate, you can re-initialize your ConnectionPool as follows (only
    applicable for Python 2.7.9+):

    >>> db.resource.session.disable_ssl_verification()
    """

    def __init__(self, server, name, check=True):
        self._name = name
        self._server = server
        if check:
            self.check()

    @classmethod
    def from_url(cls, url, **options):
        """
        Initialize a database object from a URL instead of a `Server` object.

        Also works with just a name, in which case the server url defaults to the default URL.
        """
        parsed_url = furl.furl(url)
        if len(parsed_url.path.segments) > 1:
            raise ValueError("URL contains more than one path.")
        db_name = parsed_url.path.segments[0]
        parsed_url.remove(path=True)
        server = Server(url=parsed_url.url or DEFAULT_BASE_URL)
        return cls(server=server, name=db_name, **options)

    @property
    def name(self):
        return self._name
    
    @property
    def server(self):
        return self._server

    @property
    def path(self):
        return furl.Path(quote(self.name))

    def exists(self):
        try:
            self.server.session.head(self.path)
        except exceptions.HTTPNotFound:
            return False
        return True

    def check(self):
        if not self.exists():
            raise exceptions.MissingDatabase("Database does not exist")

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, self.name)

    def __contains__(self, id):
        """Return whether the database contains a document with the specified
        ID.

        :param id: the document ID
        :return: `True` if a document with the ID exists, `False` otherwise
        """
        try:
            self.server.session.head(self.path.add([id]))
        except exceptions.HTTPNotFound:
            return False
        return True

    def __iter__(self):
        """Return the IDs of all documents in the database."""
        try:
            return iter([item.id for item in self.view('_all_docs')])
        except exceptions.HTTPNotFound as exc:
            six.raise_from(exceptions.MissingDatabase("Database does not exist"), exc)

    def __len__(self):
        """Return the number of documents in the database."""
        try:
            return self.server.session.get(self.path).json()['doc_count']
        except exceptions.HTTPNotFound as exc:
            six.raise_from(exceptions.MissingDatabase("Database does not exist"), exc)

    def __nonzero__(self):
        """Return whether the database is available."""
        return self.exists()

    def __bool__(self):
        return self.exists()

    def __delitem__(self, id):
        """Remove the document with the specified ID from the database.

        :param id: the document ID
        """
        resp = self.server.session.head(self.path.add([id]))
        rev = resp.headers['ETag'].strip('"')
        return self.delete({'_id': id, '_rev': rev})

    def __getitem__(self, id):
        """Return the document with the specified ID.

        :param id: the document ID
        :return: a `Row` object representing the requested document
        :rtype: `Document`
        """
        try:
            return Document(self.server.session.get(self.path.add([id])).json())
        except exceptions.HTTPNotFound as exc:
            six.raise_from(exceptions.MissingDocument("Document does not exist"), exc)

    def __setitem__(self, id, content):
        """Create or update a document with the specified ID.

        :param id: the document ID
        :param content: the document content; either a plain dictionary for
                        new documents, or a `Row` object for existing
                        documents
        """
        data = self.server.session.put(self.path.add([id]), json=content).json()
        # TODO: I'm not a fan of this side-effect on given data
        content.update({'_id': data['id'], '_rev': data['rev']})

    @property
    def security(self):
        return self.server.session.get(self.path.add("_security")).json()

    @security.setter
    def security(self, doc):
        self.server.session.put(self.path.add("_security"), json=doc)

    def save(self, doc, batch=False):
        """Create a new document or update an existing document.

        If doc has no _id then the server will allocate a random ID and a new
        document will be created. Otherwise the doc's _id will be used to
        identify the document to create or update. Trying to update an existing
        document with an incorrect _rev will raise a ResourceConflict exception.

        Note that it is generally better to avoid saving documents with no _id
        and instead generate document IDs on the client side. This is due to
        the fact that the underlying HTTP ``POST`` method is not idempotent,
        and an automatic retry due to a problem somewhere on the networking
        stack may cause multiple documents being created in the database.

        To avoid such problems you can generate a UUID on the client side.
        Python (since version 2.5) comes with a ``uuid`` module that can be
        used for this::

            from uuid import uuid4
            doc = {'_id': uuid4().hex, 'type': 'person', 'name': 'John Doe'}
            db.save(doc)

        :param doc: the document to store
        :param options: optional args, e.g. batch='ok'
        :return: (id, rev) tuple of the save document
        :rtype: `tuple`
        """
        params = {'batch': 'ok'} if batch else {}
        try:
            data = self.server.session.post(self.path, json=doc, params=params).json()
        except exceptions.HTTPConflict as exc:
            six.raise_from(exceptions.UpdateConflict, exc)

        # TODO: I'm not a fan of this side-effect on given data
        doc['_id'] = data['id']
        rev = data.get('rev')
        # Not present for batch='ok'
        if rev:
            doc['_rev'] = data['rev']
        return doc['_id'], rev

    def cleanup(self):
        """Clean up old design document indexes.

        Remove all unused index files from the database storage area.

        :return: a boolean to indicate successful cleanup initiation
        :rtype: `bool`
        """
        data = self.server.session.post(self.path.add("_view_cleanup")).json()
        return data['ok']

    def compact(self, ddoc=None):
        """Compact the database or a design document's index.

        Without an argument, this will try to prune all old revisions from the
        database. With an argument, it will compact the index cache for all
        views in the design document specified.

        :return: a boolean to indicate whether the compaction was initiated
                 successfully
        :rtype: `bool`
        """
        # This is for legacy compatibility
        if ddoc:
            return self.compact_views(ddoc)
        # Needs empty json arguments, so that 'application/json' content-type is set
        data = self.server.session.post(self.path.add("_compact"), json={}).json()
        return data['ok']

    def compact_views(self, design_doc):
        # Needs empty json arguments, so that 'application/json' content-type is set
        data = self.server.session.post(self.path.add(["_compact", design_doc]), json={}).json()
        return data['ok']

    def copy(self, src, dest):
        """Copy the given document to create a new document.

        :param src: the ID of the document to copy, or a dictionary or
                    `Document` object representing the source document.
        :param dest: either the destination document ID as string, or a
                     dictionary or `Document` instance of the document that
                     should be overwritten.
        :return: the new revision of the destination document
        :rtype: `str`
        :since: 0.6
        """
        if not isinstance(src, util.strbase):
            if not isinstance(src, dict):
                if hasattr(src, 'items'):
                    src = dict(src.items())
                else:
                    raise TypeError('expected dict or string, got %s' %
                                    type(src))
            src_id = src['_id']
        else:
            src_id = src

        if not isinstance(dest, util.strbase):
            if not isinstance(dest, dict):
                if hasattr(dest, 'items'):
                    dest = dict(dest.items())
                else:
                    raise TypeError('expected dict or string, got %s' %
                                    type(dest))
            dest_id = dest['_id']
            dest_rev = dest.get("_rev")
        else:
            dest_id = dest
            dest_rev = None

        if dest_rev:
            destination = "{}?rev={}".format(quote(dest_id), quote(dest_rev))
        else:
            destination = quote(dest_id)

        try:
            data = self.server.session.request("COPY", self.path.add([src_id]), headers={'Destination': destination}).json()
        except exceptions.HTTPConflict as exc:
            six.raise_from(exceptions.UpdateConflict, exc)
        return data['rev']

    def delete(self, doc):
        """Delete the given document from the database.

        Use this method in preference over ``__del__`` to ensure you're
        deleting the revision that you had previously retrieved. In the case
        the document has been updated since it was retrieved, this method will
        raise a `ResourceConflict` exception.

        >>> server = Server()
        >>> db = server.create('python-tests')

        >>> doc = dict(type='Person', name='John Doe')
        >>> db['johndoe'] = doc
        >>> doc2 = db['johndoe']
        >>> doc2['age'] = 42
        >>> db['johndoe'] = doc2
        >>> db.delete(doc) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
          ...
        ResourceConflict: (u'conflict', u'Document update conflict.')

        >>> del server['python-tests']

        :param doc: a dictionary or `Document` object holding the document data
        :raise ResourceConflict: if the document was updated in the database
        :since: 0.4.1
        """
        if doc['_id'] is None:
            raise ValueError('document ID cannot be None')
        self.server.session.delete(self.path.add([doc['_id']]), params={'rev': doc['_rev']})

    def get(self,
            id,
            default=None,
            att_encoding_info=None,
            atts_since=None,
            conflicts=None,
            deleted_conflicts=None,
            latest=None,
            local_seq=None,
            meta=None,
            open_revs=None,
            rev=None,
            revs=None,
            revs_info=None
        ):
        """Return the document with the specified ID.

        :param id: the document ID
        :param default: the default value to return when the document is not
                        found
        :return: a `Document` object representing the requested document, or the given default value
                 if no document with the ID was found
        :rtype: `Document`
        """
        params = {}
        if att_encoding_info is not None:
            params['att_encoding_info'] = _jsons(att_encoding_info)
        if atts_since is not None:
            params['atts_since'] = _jsons(bool(atts_since))
        if conflicts is not None:
            params['conflicts'] = _jsons(bool(conflicts))
        if deleted_conflicts is not None:
            params['deleted_conflicts'] = _jsons(bool(deleted_conflicts))
        if latest is not None:
            params['latest'] = _jsons(bool(latest))
        if local_seq is not None:
            params['local_seq'] = _jsons(bool(local_seq))
        if meta is not None:
            params['meta'] = _jsons(bool(meta))
        if open_revs is not None:
            params['open_revs'] = _jsons(open_revs)
        if rev is not None:
            params['rev'] = rev
        if revs is not None:
            params['revs'] = _jsons(bool(revs))
        if revs_info is not None:
            params['revs_info'] = _jsons(bool(revs_info))

        try:
            return self.server.session.get(self.path.add([id]), params=params).json()
        except exceptions.HTTPNotFound:
            return default

    def revisions(self, id, **options):
        """Return all available revisions of the given document.

        :param id: the document ID
        :return: an iterator over Document objects, each a different revision,
                 in reverse chronological order, if any were found
        """
        # Example response from CouchDB
        # {u'_id': u'test_id',
        #  u'_rev': u'6-c28221743000231ebf199462761404af',
        #  u'_revisions': {u'ids': [u'c28221743000231ebf199462761404af',
        #    u'3469ea465a1a070e7182af962a497689',
        #    u'725610bd19e28d020c45ce8ac0a8ee10',
        #    u'7608c9fb036ae43de6008283333760f9',
        #    u'65daf468bbf4f394f6ed216e056f2c7c',
        #    u'76c5d3a55599332c423ddf4cdcad828b'],
        #   u'start': 6},
        #  u'data': u'THIS IS SOME DATA'}

        data = self.get(id, revs=True)
        if data is None:
            return
        startrev = data['_revisions']['start']
        for index, rev in enumerate(data['_revisions']['ids']):
            target_rev = '%d-%s' % (startrev - index, rev)
            doc = self.get(id, rev=target_rev)
            if doc is None:
                return
            yield Document(doc)


    def info(self, ddoc=None):
        """Return information about the database or design document as a
        dictionary.

        Without an argument, returns database information. With an argument,
        return information for the given design document.

        The returned dictionary exactly corresponds to the JSON response to
        a ``GET`` request on the database or design document's info URI.

        :return: a dictionary of database properties
        :rtype: ``dict``
        :since: 0.4
        """
        if ddoc is not None:
            data = self.server.session.get(self.path.add(['_design', ddoc, '_info'])).json()
        else:
            data = self.server.session.get(self.path).json()
        return data

    def delete_attachment(self, doc, filename, batch=False):
        """Delete the specified attachment.

        Note that the provided `doc` is required to have a ``_rev`` field.
        Thus, if the `doc` is based on a view row, the view row would need to
        include the ``_rev`` field.

        :param doc: the dictionary or `Document` object representing the
                    document that the attachment belongs to
        :param filename: the name of the attachment file
        :since: 0.4.1
        """
        params = {'rev': doc['_rev']}
        if batch:
            params = {'batch': 'ok'}
        data = self.server.session.delete(self.path.add([doc['_id'], filename]), params=params).json()
        doc['_rev'] = data['rev']

    def get_attachment(self, id_or_doc, filename, default=None, rev=None):
        """Return an attachment from the specified doc id and filename.

        :param id_or_doc: either a document ID or a dictionary or `Document`
                          object representing the document that the attachment
                          belongs to
        :param filename: the name of the attachment file
        :param default: default value to return when the document or attachment
                        is not found
        :return: a file-like object with read and close methods, or the value
                 of the `default` argument if the attachment is not found
        :since: 0.4.1
        """
        try:
            id = id_or_doc['_id']
            if rev is None:
                rev = id_or_doc.get('_rev', None)
        except TypeError:
            id = id_or_doc

        params = {'rev': rev} if rev is not None else {}
        try:
            return io.BytesIO(self.server.session.get(self.path.add([id, filename]), params=params).content)
        except exceptions.HTTPNotFound:
            return default

    def put_attachment(self, doc, content, filename=None, content_type=None):
        """Create or replace an attachment.

        Note that the provided `doc` is required to have a ``_rev`` field. Thus,
        if the `doc` is based on a view row, the view row would need to include
        the ``_rev`` field.

        :param doc: the dictionary or `Document` object representing the
                    document that the attachment should be added to
        :param content: the content to upload, either a file-like object or
                        a string
        :param filename: the name of the attachment file; if omitted, this
                         function tries to get the filename from the file-like
                         object passed as the `content` argument value
        :param content_type: content type of the attachment; if omitted, the
                             MIME type is guessed based on the file name
                             extension
        :since: 0.4.1
        """
        if filename is None:
            try:
                filename = os.path.basename(content.name)
            except AttributeError as exc:
                six.raise_from(ValueError('Could not determine filename from file object'), exc)
        if not content_type:
            (content_type, enc) = mimetypes.guess_type(filename, strict=False)
            if not content_type:
                content_type = BIN_MIME
        headers = {"Content-Type": content_type, "If-Match": doc["_rev"]}
        resp = self.server.session.put(self.path.add([doc['_id'], filename]), data=content, headers=headers)
        doc['_rev'] = resp.json()['rev']
        return doc["_rev"]

    def find(self, mango_query, wrapper=None):
        """Execute a mango find-query against the database.

        Note: only available for CouchDB version >= 2.0.0

        More information on the `mango_query` structure can be found here:
          http://docs.couchdb.org/en/master/api/database/find.html#find-selectors

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> db['johndoe'] = dict(type='Person', name='John Doe')
        >>> db['maryjane'] = dict(type='Person', name='Mary Jane')
        >>> db['gotham'] = dict(type='City', name='Gotham City')
        >>> mango = {'selector': {'type': 'Person'},
        ...          'fields': ['name'],
        ...          'sort':[{'name': 'asc'}]}
        >>> for row in db.find(mango):                          # doctest: +SKIP
        ...    print(row['name'])                               # doctest: +SKIP
        John Doe
        Mary Jane
        >>> del server['python-tests']

        :param mango_query: a dictionary describing criteria used to select
                            documents
        :param wrapper: an optional callable that should be used to wrap the
                        resulting documents
        :return: the query results as a list of `Document` (or whatever `wrapper` returns)
        """
        data = self.server.session.post(self.path.add("_find"), json=mango_query).json()
        return map(wrapper or Document, data.get('docs', []))

    def explain(self, mango_query):
        """Explain a mango find-query.

        Note: only available for CouchDB version >= 2.0.0

        More information on the `mango_query` structure can be found here:
          http://docs.couchdb.org/en/master/api/database/find.html#db-explain

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> db['johndoe'] = dict(type='Person', name='John Doe')
        >>> db['maryjane'] = dict(type='Person', name='Mary Jane')
        >>> db['gotham'] = dict(type='City', name='Gotham City')
        >>> mango = {'selector': {'type': 'Person'}, 'fields': ['name']}
        >>> db.explain(mango)                          #doctest: +ELLIPSIS +SKIP
        {...}
        >>> del server['python-tests']

        :param mango_query: a `dict` describing criteria used to select
                            documents
        :return: the query results as a list of `Document` (or whatever
                 `wrapper` returns)
        :rtype: `dict`
        """
        data = self.server.session.post(self.path.add("_explain"), json=mango_query).json()
        return data

    def index(self):
        """Get an object to manage the database indexes.

        :return: an `Indexes` object to manage the databes indexes
        :rtype: `Indexes`
        """
        raise NotImplementedError
        return Indexes(self.resource('_index'))

    def update(self, documents, new_edits=True):
        """Perform a bulk update or insertion of the given documents using a
        single HTTP request.

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> for doc in db.update([
        ...     Document(type='Person', name='John Doe'),
        ...     Document(type='Person', name='Mary Jane'),
        ...     Document(type='City', name='Gotham City')
        ... ]):
        ...     print(repr(doc)) #doctest: +ELLIPSIS
        (True, u'...', u'...')
        (True, u'...', u'...')
        (True, u'...', u'...')

        >>> del server['python-tests']

        The return value of this method is a list containing a tuple for every
        element in the `documents` sequence. Each tuple is of the form
        ``(success, docid, rev_or_exc)``, where ``success`` is a boolean
        indicating whether the update succeeded, ``docid`` is the ID of the
        document, and ``rev_or_exc`` is either the new document revision, or
        an exception instance (e.g. `ResourceConflict`) if the update failed.

        If an object in the documents list is not a dictionary, this method
        looks for an ``items()`` method that can be used to convert the object
        to a dictionary. Effectively this means you can also use this method
        with `mapping.Document` objects.

        :param documents: a sequence of dictionaries or `Document` objects, or
                          objects providing a ``items()`` method that can be
                          used to convert them to a dictionary
        :return: an iterable over the resulting documents
        :rtype: ``list``

        :since: version 0.2
        """
        docs = []
        for doc in documents:
            if isinstance(doc, dict):
                docs.append(doc)
            elif hasattr(doc, 'items'):
                docs.append(dict(doc.items()))
            else:
                raise TypeError('expected dict, got %s' % type(doc))

        payload = {
            'docs': docs,
            'new_edits': new_edits,
        }
        data = self.server.session.post(self.path.add("_bulk_docs"), json=payload).json()

        results = []
        for idx, result in enumerate(data):
            if 'error' in result:
                if result['error'] == 'conflict':
                    exc_type = exceptions.UpdateConflict
                else:
                    # XXX: Any other error types mappable to exceptions here?
                    exc_type = exceptions.CouchDBException
                results.append((False, result['id'],
                                exc_type(result['reason'])))
            else:
                doc = documents[idx]
                if isinstance(doc, dict): # XXX: Is this a good idea??
                    doc.update({'_id': result['id'], '_rev': result['rev']})
                results.append((True, result['id'], result['rev']))

        return results

    def purge(self, docs):
        """Perform purging (complete removing) of the given documents.

        Uses a single HTTP request to purge all given documents. Purged
        documents do not leave any meta-data in the storage and are not
        replicated.
        """
        content = {}
        for doc in docs:
            if isinstance(doc, dict):
                content[doc['_id']] = [doc['_rev']]
            elif hasattr(doc, 'items'):
                doc = dict(doc.items())
                content[doc['_id']] = [doc['_rev']]
            else:
                raise TypeError('expected dict, got %s' % type(doc))
        return self.server.session.post(self.path.add("_purge"), json=content).json()

    def view(self, name, **options):
        """Execute a predefined view.

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> db['gotham'] = dict(type='City', name='Gotham City')

        >>> for row in db.view('_all_docs'):
        ...     print(row.id)
        gotham

        >>> del server['python-tests']

        :param name: the name of the view; for custom views, use the format
                     ``design_docid/viewname``, that is, the document ID of the
                     design document and the name of the view, separated by a
                     slash
        :param options: optional query string parameters
        :return: the view results
        :rtype: `ViewResults`
        """
        if name.startswith('_'):
            design_doc_name = None
            view_name = name
        else:
            design_doc_name, view_name = name.split('/', 1)
        return self.query_view(design_doc=design_doc_name, view_name=view_name, **options)

    def query_view(
        self,
        design_doc,
        view_name,
        key=None,
        keys=None,
        startkey=None,
        endkey=None,
        skip=None,
        limit=None,
        sorted=True,
        descending=False,
        group=False,
        group_level=None,
        reduce=None,
        include_docs=False,
        update=None,
    ):
        """Query a view index to obtain data and/or documents.

        Keyword arguments:

        - `key`: Return only rows that match the specified key.
        - `keys`: Return only rows where they key matches one of
          those specified as a list.
        - `startkey`: Return rows starting with the specified key.
        - `endkey`: Stop returning rows when the specified key is reached.
        - `skip`: Skip the number of rows before starting to return rows.
        - `limit`: Limit the number of rows returned.
        - `sorted=True`: Sort the rows by the key of each returned row.
          The items `total_rows` and `offset` are not available if
          set to `False`.
        - `descending=False`: Return the rows in descending order.
        - `group=False`: Group the results using the reduce function of
          the design document to a group or a single row. If set to `True`
          implies `reduce` to `True` and defaults the `group_level`
          to the maximum.
        - `group_level`: Specify the group level to use. Implies `group`
          is `True`.
        - `reduce`: If set to `False` then do not use the reduce function
          if there is one defined in the design document. Default is to
          use the reduce function if defined.
        - `include_docs=False`: If `True`, include the document for each row.
          This will force `reduce` to `False`.
        - `update="true"`: Whether or not the view should be updated prior to
          returning the result. Supported value are `"true"`, `"false"`
          and `"lazy"`.

        Returns a ViewResult instance, containing the following attributes:

        - `rows`: the list of Row instances.
        - `offset`: the offset used for the set of rows.
        - `total_rows`: the total number of rows selected.

        A Row object contains the following attributes:

        - `id`: the identifier of the document, if any.
        - `key`: the key for the index row.
        - `value`: the value for the index row.
        - `doc`: the document, if any.
        """
        params = {}
        if startkey is not None:
            params["startkey"] = _jsons(startkey)
        if key is not None:
            params["key"] = _jsons(key)
        if keys is not None:
            params["keys"] = _jsons(keys)
        if endkey is not None:
            params["endkey"] = _jsons(endkey)
        if skip is not None:
            params["skip"] = _jsons(skip)
        if limit is not None:
            params["limit"] = _jsons(limit)
        if not sorted:
            params["sorted"] = _jsons(False)
        if descending:
            params["descending"] = _jsons(True)
        if group:
            params["group"] = _jsons(True)
        if group_level is not None:
            params["group_level"] = _jsons(group_level)
        if reduce is not None:
            params["reduce"] = _jsons(bool(reduce))
        if include_docs:
            params["include_docs"] = _jsons(True)
            params["reduce"] = _jsons(False)
        if update is not None:
            assert update in ["true", "false", "lazy"]
            params["update"] = update

        if design_doc is None:
            assert view_name.startswith("_")
            path = self.path.add(view_name)
        else:
            path = self.path.add(["_design", design_doc, "_view", view_name])

        data = self.server.session.get(path, params=params).json()
        return views.ViewResult(
            [
                views.Row(r.get("id"), r.get("key"), r.get("value"), r.get("error"), r.get("doc"))
                for r in data.get("rows", [])
            ],
            data.get("offset"),
            data.get("total_rows"),
        )

    def iterview(self, name, batch, wrapper=None, **options):
        """Iterate the rows in a view, fetching rows in batches and yielding
        one row at a time.

        Since the view's rows are fetched in batches any rows emitted for
        documents added, changed or deleted between requests may be missed or
        repeated.

        :param name: the name of the view; for custom views, use the format
                     ``design_docid/viewname``, that is, the document ID of the
                     design document and the name of the view, separated by a
                     slash.
        :param batch: number of rows to fetch per HTTP request.
        :param wrapper: an optional callable that should be used to wrap the
                        result rows
        :param options: optional query string parameters
        :return: row generator
        """
        raise NotImplementedError
        # Check sane batch size.
        if batch <= 0:
            raise ValueError('batch must be 1 or more')
        # Save caller's limit, it must be handled manually.
        limit = options.get('limit')
        if limit is not None and limit <= 0:
            raise ValueError('limit must be 1 or more')
        while True:

            loop_limit = min(limit or batch, batch)
            # Get rows in batches, with one extra for start of next batch.
            options['limit'] = loop_limit + 1
            rows = list(self.view(name, wrapper, **options))

            # Yield rows from this batch.
            for row in itertools.islice(rows, loop_limit):
                yield row

            # Decrement limit counter.
            if limit is not None:
                limit -= min(len(rows), batch)

            # Check if there is nothing else to yield.
            if len(rows) <= batch or (limit is not None and limit == 0):
                break

            # Update options with start keys for next loop.
            options.update(startkey=rows[-1]['key'],
                           startkey_docid=rows[-1]['id'], skip=0)

    def show(self, name, docid=None, **options):
        """Call a 'show' function.

        :param name: the name of the show function in the format
                     ``designdoc/showname``
        :param docid: optional ID of a document to pass to the show function.
        :param options: optional query string parameters
        :return: (headers, body) tuple, where headers is a dict of headers
                 returned from the show function and body is a readable
                 file-like instance
        """
        raise NotImplementedError
        path = _path_from_name(name, '_show')
        if docid:
            path.append(docid)
        status, headers, body = self.resource(*path).get(**options)
        return headers, body

    def list(self, name, view, **options):
        """Format a view using a 'list' function.

        :param name: the name of the list function in the format
                     ``designdoc/listname``
        :param view: the name of the view in the format ``designdoc/viewname``
        :param options: optional query string parameters
        :return: (headers, body) tuple, where headers is a dict of headers
                 returned from the list function and body is a readable
                 file-like instance
        """
        raise NotImplementedError
        path = _path_from_name(name, '_list')
        path.extend(view.split('/', 1))
        _, headers, body = _call_viewlike(self.resource(*path), options)
        return headers, body

    def update_doc(self, name, docid=None, **options):
        """Calls server side update handler.

        :param name: the name of the update handler function in the format
                     ``designdoc/updatename``.
        :param docid: optional ID of a document to pass to the update handler.
        :param options: additional (optional) params to pass to the underlying
                        http resource handler, including ``headers``, ``body``,
                        and ```path```. Other arguments will be treated as
                        query string params. See :class:`couchdb.http.Resource`
        :return: (headers, body) tuple, where headers is a dict of headers
                 returned from the list function and body is a readable
                 file-like instance
        """
        raise NotImplementedError
        path = _path_from_name(name, '_update')
        if docid is None:
            func = self.resource(*path).post
        else:
            path.append(docid)
            func = self.resource(*path).put
        _, headers, body = func(**options)
        return headers, body

    def _changes(self, **opts):
        raise NotImplementedError
        # use streaming `get` and `post` methods
        if opts.get('filter') == '_selector':
            selector = opts.pop('_selector', None)
            _, _, data = self.resource.post('_changes', selector, **opts)
        else:
            _, _, data = self.resource.get('_changes', **opts)
        lines = data.iterchunks()
        for ln in lines:
            if not ln: # skip heartbeats
                continue
            doc = json.loads(ln.decode('utf-8'))
            if 'last_seq' in doc: # consume the rest of the response if this
                for ln in lines:  # was the last line, allows conn reuse
                    pass
            yield doc

    def changes(self, **opts):
        """Retrieve a changes feed from the database.

        :param opts: optional query string parameters
        :return: an iterable over change notification dicts
        """
        raise NotImplementedError
        if opts.get('feed') == 'continuous':
            return self._changes(**opts)

        if opts.get('filter') == '_selector':
            selector = opts.pop('_selector', None)
            _, _, data = self.resource.post_json('_changes', selector, **opts)
        else:
            _, _, data = self.resource.get_json('_changes', **opts)
        return data

def _path_from_name(name, type):
    """Expand a 'design/foo' style name to its full path as a list of
    segments.
    """
    if name.startswith('_'):
        return name.split('/')
    design, name = name.split('/', 1)
    return ['_design', design, type, name]


class Document(dict):
    """Representation of a document in the database.

    This is basically just a dictionary with the two additional properties
    `id` and `rev`, which contain the document ID and revision, respectively.
    """

    def __repr__(self):
        return '<%s %r@%r %r>' % (type(self).__name__, self.id, self.rev,
                                  dict([(k,v) for k,v in self.items()
                                        if k not in ('_id', '_rev')]))

    @property
    def id(self):
        """The document ID.

        :rtype: basestring
        """
        return self.get('_id')


    @property
    def rev(self):
        """The document revision.

        :rtype: basestring
        """
        return self.get('_rev')


def _encode_view_options(options):
    """Encode any items in the options dict that are sent as a JSON string to a
    view/list function.
    """
    retval = {}
    for name, value in options.items():
        if name in ('key', 'startkey', 'endkey') \
                or not isinstance(value, util.strbase):
            value = json.dumps(value)
        retval[name] = value
    return retval


def _call_viewlike(resource, options):
    """Call a resource that takes view-like options.
    """
    if 'keys' in options:
        options = options.copy()
        keys = {'keys': options.pop('keys')}
        return resource.post_json(body=keys, **_encode_view_options(options))
    else:
        return resource.get_json(**_encode_view_options(options))


class Indexes(object):
    """Manage indexes in CouchDB 2.0.0 and later.

    More information here:
        http://docs.couchdb.org/en/2.0.0/api/database/find.html#db-index
    """

    def __init__(self, url, session=None):
        if isinstance(url, util.strbase):
            self.resource = http.Resource(url, session)
        else:
            self.resource = url

    def __setitem__(self, ddoc_name, index):
        """Add an index to the database.

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> db['johndoe'] = dict(type='Person', name='John Doe')
        >>> db['maryjane'] = dict(type='Person', name='Mary Jane')
        >>> db['gotham'] = dict(type='City', name='Gotham City')
        >>> idx = db.index()
        >>> idx['foo', 'bar'] = [{'type': 'asc'}]                #doctest: +SKIP
        >>> list(idx)                                           #doctest: +SKIP
        [{'ddoc': None,
          'def': {'fields': [{'_id': 'asc'}]},
          'name': '_all_docs',
          'type': 'special'},
         {'ddoc': '_design/foo',
          'def': {'fields': [{'type': 'asc'}]},
          'name': 'bar',
          'type': 'json'}]
        >>> idx[None, None] = [{'type': 'desc'}]      #doctest: +SKIP
        >>> list(idx)                                 #doctest: +SKIP, +ELLIPSIS
        [{'ddoc': None,
          'def': {'fields': [{'_id': 'asc'}]},
          'name': '_all_docs',
          'type': 'special'},
         {'ddoc': '_design/...',
          'def': {'fields': [{'type': 'desc'}]},
          'name': '...',
          'type': 'json'},
         {'ddoc': '_design/foo',
          'def': {'fields': [{'type': 'asc'}]},
          'name': 'bar',
          'type': 'json'}]
        >>> del server['python-tests']

        :param index: `list` of indexes to create
        :param ddoc_name: `tuple` or `list` containing first the name of the
                          design document, in which the index will be created,
                          and second name of the index. Both can be `None`.
        """
        query = {'index': {'fields': index}}
        ddoc, name = ddoc_name  # expect ddoc / name to be a slice or list
        if ddoc:
            query['ddoc'] = ddoc
        if name:
            query['name'] = name
        self.resource.post_json(body=query)

    def __delitem__(self, ddoc_name):
        """Remove an index from the database.

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> db['johndoe'] = dict(type='Person', name='John Doe')
        >>> db['maryjane'] = dict(type='Person', name='Mary Jane')
        >>> db['gotham'] = dict(type='City', name='Gotham City')
        >>> idx = db.index()
        >>> idx['foo', 'bar'] = [{'type': 'asc'}]                #doctest: +SKIP
        >>> del idx['foo', 'bar']                                #doctest: +SKIP
        >>> list(idx)                                            #doctest: +SKIP
        [{'ddoc': None,
          'def': {'fields': [{'_id': 'asc'}]},
          'name': '_all_docs',
          'type': 'special'}]
        >>> del server['python-tests']

        :param ddoc: name of the design document containing the index
        :param name: name of the index that is to be removed
        :return: `dict` containing the `id`, the `name` and the `result` of
                 creating the index
        """
        self.resource.delete_json([ddoc_name[0], 'json', ddoc_name[1]])

    def _list(self):
        _, _, data = self.resource.get_json()
        return data

    def __iter__(self):
        """Iterate all indexes of the associated database.

        >>> server = Server()
        >>> db = server.create('python-tests')
        >>> idx = db.index()
        >>> list(idx)                                            #doctest: +SKIP
        [{'ddoc': None,
          'def': {'fields': [{'_id': 'asc'}]},
          'name': '_all_docs',
          'type': 'special'}]
        >>> del server['python-tests']

        :return: iterator yielding `dict`'s describing each index
        """
        return iter(self._list()['indexes'])
