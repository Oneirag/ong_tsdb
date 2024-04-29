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
