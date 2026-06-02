import unittest
from ong_tsdb.check_versions import check_version


class TestVersion(unittest.TestCase):
    """Simple tests for version compatibility"""

    def test_version(self):
        test_set = [
            dict(server="0.6.2", client="0.6.2", expected=True),
            dict(server="0.6.1", client="0.6.2", expected=False),
            dict(server="0.7.1", client="0.6.2", expected=True),
            dict(server="0.5.1", client="0.6.2", expected=False),
            dict(server="0.5.1", client="0.6.2", expected=False),
            dict(server=None, client="0.6.2", expected=False),
            dict(server="0.7.10", client="0.7.9", expected=True),
            dict(server="0.7.9", client="0.7.10", expected=False),
            dict(server="0.7.3+local", client="0.7.3", expected=True),
            dict(server="0.7.3.dev1", client="0.7.3", expected=True),
            dict(server="0.7.3-rc1", client="0.7.3", expected=True),
        ]
        for test in test_set:
            with self.subTest(**test):
                server, client, expected = test.values()
                version_check = check_version(server, client)
                self.assertEqual(
                    version_check, expected, msg=f"Bad expected for {test}"
                )
