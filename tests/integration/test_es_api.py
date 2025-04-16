"""Integration tests for es_testbed.es_utils"""

# pylint: disable=C0115,C0116,R0903,R0913,R0917,W0212
import pytest
from es_testbed.es_api import create_index, delete, verify
from . import INDEX1, SETTINGS


def test_delete(client):
    create_index(client, INDEX1, settings=SETTINGS)  # Use the helper function instead
    assert delete(client, 'index', INDEX1)


@pytest.mark.parametrize('name,idxcfg', [(INDEX1, {})], indirect=True)
def test_verify_false(client, create_idx):
    """Verify index deleted is actually deleted

    A False return value from verify() means the index still exists.
    """
    assert create_idx.name == INDEX1
    assert not verify(client, 'index', INDEX1)


def test_verify_true(client):
    """Verify true response

    A True return value from verify() means the index was deleted.
    """
    assert verify(client, 'index', INDEX1)
