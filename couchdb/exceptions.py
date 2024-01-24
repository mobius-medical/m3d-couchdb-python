class CouchDBException(Exception):
    """There was an ambiguous error interacting with CouchDB."""
    pass


class UpdateConflict(CouchDBException):
    """A revision conflict occurred."""
    pass


class MissingResource(CouchDBException):
    """A requested resource (database, document, view) does not exist"""
    pass


class MissingDocument(MissingResource):
    """A requested document does not exist."""
    pass


class MissingDatabase(MissingResource):
    """A requested database does not exist."""
    pass


class MissingView(MissingResource):
    """A requested view does not exist"""
    pass


class DatabaseExists(CouchDBException):
    """Could not create a database, it exists already."""
    pass


class LoginFailed(CouchDBException):
    """Could not authenticate the provided user."""
    pass


class RequestsException(CouchDBException):
    """There was an ambiguous exception that occurred while handling your request."""
    pass


class Timeout(RequestsException):
    """The request timed out."""
    pass


class HTTPError(RequestsException):
    """An HTTP error occurred."""
    def __init__(self, status_code, message=None):
        self.status_code = status_code
        self.message = message or "HTTP error {status_code}".format(status_code=status_code)
        super(HTTPError, self).__init__(self.message)


class HTTPBadRequest(HTTPError):
    """400 Bad Request"""
    status_code = 400

    def __init__(self, message="Bad Request"):
        super(HTTPBadRequest, self).__init__(self.__class__.status_code, message)


class HTTPUnauthorized(HTTPError):
    """401 Unauthorized"""
    status_code = 401

    def __init__(self, message="Unauthorized"):
        super(HTTPUnauthorized, self).__init__(self.__class__.status_code, message)


class HTTPForbidden(HTTPError):
    """403 Forbidden"""
    status_code = 403

    def __init__(self, message="Forbidden"):
        super(HTTPForbidden, self).__init__(self.__class__.status_code, message)


class HTTPNotFound(HTTPError):
    """404 Not Found"""
    status_code = 404

    def __init__(self, message="Not Found"):
        super(HTTPNotFound, self).__init__(self.__class__.status_code, message)


class HTTPConflict(HTTPError):
    status_code = 409

    def __init__(self, message="Conflict"):
        super(HTTPConflict, self).__init__(self.__class__.status_code, message)


class HTTPPreconditionFailed(HTTPError):
    status_code = 412

    def __init__(self, message="Precondition failed"):
        super(HTTPPreconditionFailed, self).__init__(self.__class__.status_code, message)


_http_error_lookup = {
    exc.status_code: exc for exc in [HTTPBadRequest, HTTPUnauthorized, HTTPForbidden, HTTPNotFound, HTTPConflict, HTTPPreconditionFailed]
}


def http_error_lookup(status_code, message=None):
    if status_code in _http_error_lookup:
        return _http_error_lookup[status_code]()
    else:
        return HTTPError(status_code=status_code, message=message)