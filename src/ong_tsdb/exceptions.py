class OngTsdbClientBaseException(Exception):
    """Base Exception for the exceptions to OngTsdbClient object"""


class NotAuthorizedException(OngTsdbClientBaseException):
    """Exception raised when 401 error is received from server"""

    pass


class ProxyNotAuthorizedException(OngTsdbClientBaseException):
    """Exception raised when 407 error is received from server. Stores failed response for later use"""

    def __init__(self, msg, response):
        super().__init__(msg)
        self.response = response


class ServerDownException(OngTsdbClientBaseException):
    """Exception raised when cannot connect to server"""

    pass


class WrongAddressException(OngTsdbClientBaseException):
    """Raised when 404 error is received"""

    pass


class WrongServerVersion(OngTsdbClientBaseException):
    """Raised when server and client version do not match"""

    pass


# ---- Server-side exceptions (originally declared in ong_tsdb.database) ----
# These are re-exported from ong_tsdb.database for backward compatibility.
# Inheriting from OngTsdbClientBaseException means generic `except
# OngTsdbClientBaseException` clauses on the client still capture them.


class OngTSDBbBaseException(OngTsdbClientBaseException):
    """Base class for server-side exceptions."""


class NotAuthorizedException(OngTsdbClientBaseException):
    """Raised on the server side when a key is invalid for the requested action."""


class ElementAlreadyExistsException(OngTSDBbBaseException):
    pass


class ElementNotFoundException(OngTSDBbBaseException):
    pass


class InvalidDataWriteException(OngTSDBbBaseException):
    pass
