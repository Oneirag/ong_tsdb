from ong_tsdb import __version__
from ong_tsdb.exceptions import WrongServerVersion


def _parse_version(version: str) -> tuple:
    # Drop PEP 440 local version label ("+foo") and pre/post/dev/release
    # separators ("-rc1", "-dev1") so they do not affect the comparison.
    version = version.split("+", 1)[0].split("-", 1)[0]
    return tuple(int(p) for p in version.split(".") if p.isdigit())


def check_version(server_version: str, client_version: str = __version__) -> bool:
    """Checks if client version is compatible"""
    server_version = server_version or "0.0.0"
    version_ok = _parse_version(server_version) >= _parse_version(client_version)
    return version_ok


def check_version_and_raise(server_version: str = None):
    if not check_version(server_version):
        raise WrongServerVersion(
            f"Server version {server_version} do not match client version {__version__}"
        )
