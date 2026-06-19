from ong_tsdb import __version__
from ong_tsdb.exceptions import WrongServerVersion


def _parse_version(version: str) -> tuple:
    return tuple(map(int, version.split(".")[:-1]))


def check_version(server_version: str, client_version: str = __version__) -> bool:
    """Checks if client version is compatible """
    server_version = server_version or "0.0.0"
    version_ok = _parse_version(server_version) >= _parse_version(client_version)
    return version_ok


def check_version_and_raise(server_version: str = None):
    if not check_version(server_version):
        raise WrongServerVersion(f"Server version {server_version} "
                                 f"do not match client version {__version__}")
