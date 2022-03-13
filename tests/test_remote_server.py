from ong_tsdb.client import OngTsdbClient, config
from ong_tsdb.exceptions import NotAuthorizedException, ProxyNotAuthorizedException
import unittest


class TestRemoteServer(unittest.TestCase):
    # url = "https://dev-ongtsdb.neirapinuela.es"
    url = "https://ongtsdb.neirapinuela.es"

    @classmethod
    def setUpClass(cls) -> None:
        cls.username = input("Username: ")
        cls.password = input("Password: ")
        cls.mfa_code = input("MFA code: ")

        cls.client = OngTsdbClient(cls.url, config("read_token"), retry_total=1, retry_connect=1,
                                   proxy_auth_body=dict(username=cls.username, password=cls.password,
                                                        mfa_code=cls.mfa_code))

    def test_000_connect_to_remote_server(self):
        """Tests that can connect to external server with MFA authentication"""
        # Exception should be risen due to lack of authorization
        with self.assertRaises(ProxyNotAuthorizedException) as ctx:
            _ = OngTsdbClient(self.url, config("read_token"), retry_total=1, retry_connect=1)
        print(self.client)
        print(self.client.get_metrics("commodity_data", "Omip"))

    def test_1000_change_token(self):
        """Tests that token can be changed from 'read_token' to 'admin_token'.
        To do so, this test tries to create a DB with a read_token (exception would be risen), switches to admin_token
        and creates and deletes database.
         """

        fake_db = "fake_db_that_only_exists for tests"
        read_token = config("read_token")
        admin_token = config("remote_admin_token")
        if not self.client.exist_db(fake_db):
            # Client has read_token, cannot create a db
            self.assertEqual(self.client.token, read_token, "Invalid token, it is not a read client")
            with self.assertRaises(NotAuthorizedException) as ctx:
                self.client.create_db(fake_db)
            # Now upgrade to admin token, create a db (and then delete it)
            self.client.update_token(admin_token)
            self.assertEqual(self.client.token, admin_token, "Token did not change")
            self.client.create_db(fake_db)
            # Cleanup
            self.client.delete_db(fake_db)


if __name__ == '__main__':
    unittest.main()
