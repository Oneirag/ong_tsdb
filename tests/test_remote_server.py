from ong_tsdb.client import OngTsdbClient, config, ProxyNotAuthorizedException
import unittest


class TestRemoteServer(unittest.TestCase):

    def test_remote_server(self):
        """Tests that can connect to external server with MFA authentication"""

        url = "https://dev-ongtsdb.neirapinuela.es"
        url = "https://ongtsdb.neirapinuela.es"

        # Exception should be risen due to lack of authorization
        with self.assertRaises(ProxyNotAuthorizedException) as ctx:
            client = OngTsdbClient(url, config("read_token"), retry_total=1, retry_connect=1)

        username = input("Username: ")
        password = input("Password: ")
        mfa_code = input("MFA code: ")

        client = OngTsdbClient(url, config("read_token"), retry_total=1, retry_connect=1,
                               proxy_auth_body=dict(username=username, password=password, mfa_code=mfa_code))
        print(client)
        print(client.get_metrics("commodity_data", "Omip"))


if __name__ == '__main__':
    unittest.main()