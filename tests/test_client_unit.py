"""Unit tests for OngTsdbClient that mock urllib3 to avoid a real server.

These tests do not require a running ong_tsdb server. They patch the
OngTsdbClient.http pool to return canned responses, so we can exercise
the error-handling paths (auth, 404, connection failure, ...) without
any network I/O.
"""

from unittest import TestCase, main
from unittest.mock import MagicMock, patch

from ong_tsdb.client import OngTsdbClient
from ong_tsdb.exceptions import (
    NotAuthorizedException,
    ServerDownException,
    WrongAddressException,
)


def _mock_response(status, body=b"", content_type="application/json"):
    """Build a fake urllib3-like response object."""
    r = MagicMock()
    r.status = status
    r.data = body
    r.headers = {"Content-Type": content_type}
    return r


def _client():
    """Build a client with a mocked http pool and config_reload stubbed."""
    with patch.object(OngTsdbClient, "config_reload", return_value=True):
        c = OngTsdbClient(url="http://localhost:5000", token="x")
    # Replace the http pool with a MagicMock so no real HTTP happens
    c.http = MagicMock()
    return c


# -----------------------------------------------------------------------
# E3: client._request should not swallow application-level errors
# -----------------------------------------------------------------------


class TestRequestErrorHandling(TestCase):
    def setUp(self):
        self.client = _client()

    def test_request_propagates_wrong_address(self):
        # The server returns 404 for a missing db; _request must raise
        # WrongAddressException, NOT return None.
        self.client.http.request.return_value = _mock_response(404)
        with self.assertRaises(WrongAddressException):
            self.client._request("get", "http://localhost:5000/db/no_such")

    def test_request_propagates_unauthorized(self):
        # The 401 path tries to parse retval.data as JSON; give it a
        # valid empty-ish JSON body so the json branch succeeds and
        # control falls through to the NotAuthorizedException raise.
        self.client.http.request.return_value = _mock_response(
            401, body=b"{}", content_type="application/json"
        )
        with self.assertRaises(NotAuthorizedException):
            self.client._request("get", "http://localhost:5000/db/x")

    def test_request_raises_server_down_on_connection_error(self):
        from urllib3.exceptions import ConnectionError

        self.client.http.request.side_effect = ConnectionError("nope")
        with self.assertRaises(ServerDownException):
            self.client._request("get", "http://localhost:5000/db/x")

    def test_request_does_not_swallow_unexpected_exception(self):
        # Anything else should propagate, not be silently turned into None.
        self.client.http.request.side_effect = RuntimeError("oops")
        with self.assertRaises(RuntimeError):
            self.client._request("get", "http://localhost:5000/db/x")


# -----------------------------------------------------------------------
# E3: exist_* / delete_* should only catch WrongAddressException
# -----------------------------------------------------------------------


class TestExistAndDelete(TestCase):
    def setUp(self):
        self.client = _client()

    def test_exist_db_returns_false_on_404(self):
        self.client.http.request.return_value = _mock_response(404)
        self.assertFalse(self.client.exist_db("missing_db"))

    def test_exist_db_returns_true_on_200(self):
        self.client.http.request.return_value = _mock_response(200)
        self.assertTrue(self.client.exist_db("present_db"))

    def test_exist_db_propagates_not_authorized(self):
        self.client.http.request.return_value = _mock_response(
            401, body=b"{}", content_type="application/json"
        )
        with self.assertRaises(NotAuthorizedException):
            self.client.exist_db("some_db")

    def test_exist_db_propagates_server_down(self):
        from urllib3.exceptions import ConnectionError

        self.client.http.request.side_effect = ConnectionError("nope")
        with self.assertRaises(ServerDownException):
            self.client.exist_db("some_db")

    def test_delete_db_returns_false_on_404(self):
        self.client.http.request.return_value = _mock_response(404)
        self.assertFalse(self.client.delete_db("missing_db"))

    def test_delete_db_returns_true_on_success(self):
        self.client.http.request.return_value = _mock_response(200)
        self.assertTrue(self.client.delete_db("db"))

    def test_delete_db_propagates_not_authorized(self):
        self.client.http.request.return_value = _mock_response(
            401, body=b"{}", content_type="application/json"
        )
        with self.assertRaises(NotAuthorizedException):
            self.client.delete_db("db")

    def test_exist_sensor_returns_false_on_404(self):
        self.client.http.request.return_value = _mock_response(404)
        self.assertFalse(self.client.exist_sensor("db", "missing_sensor"))

    def test_exist_sensor_propagates_server_down(self):
        from urllib3.exceptions import ConnectionError

        self.client.http.request.side_effect = ConnectionError("nope")
        with self.assertRaises(ServerDownException):
            self.client.exist_sensor("db", "sensor")

    def test_delete_sensor_propagates_not_authorized(self):
        self.client.http.request.return_value = _mock_response(
            401, body=b"{}", content_type="application/json"
        )
        with self.assertRaises(NotAuthorizedException):
            self.client.delete_sensor("db", "sensor")


if __name__ == "__main__":
    main()
