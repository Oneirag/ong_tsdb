"""Verifies that exception classes are exported from both modules and resolve
to the same object (so callers using either import path are compatible)."""

from ong_tsdb.database import (
    ElementAlreadyExistsException as srv_exists,
    ElementNotFoundException as srv_nf,
    InvalidDataWriteException as srv_inv,
    NotAuthorizedException as srv_na,
    OngTSDBbBaseException as srv_base,
)
from ong_tsdb.exceptions import (
    ElementAlreadyExistsException as cli_exists,
    ElementNotFoundException as cli_nf,
    InvalidDataWriteException as cli_inv,
    NotAuthorizedException as cli_na,
    OngTSDBbBaseException as cli_base,
)


def test_not_authorized_is_same_object():
    assert srv_na is cli_na


def test_base_exception_is_same_object():
    assert srv_base is cli_base


def test_all_classes_available_both_paths():
    for srv, cli in (
        (srv_exists, cli_exists),
        (srv_nf, cli_nf),
        (srv_inv, cli_inv),
    ):
        assert srv is cli
