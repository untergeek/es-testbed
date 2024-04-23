"""Top-level conftest.py"""
from os import environ
import random
import string
import pytest
from es_client import Builder
from es_client.helpers.logging import set_logging

LOGLEVEL = 'DEBUG'

@pytest.fixture(scope="session")
def client():
    """Return an Elasticsearch client"""
    host = environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
    set_logging({'loglevel': LOGLEVEL, 'blacklist': ['elastic_transport', 'urllib3']})
    builder = Builder(configdict={'elasticsearch': {'client': {'hosts': host}}})
    builder.connect()
    return builder.client

@pytest.fixture(scope="module")
def prefix():
    """Return a random prefix"""
    return randomstr(length=8, lowercase=True)

@pytest.fixture(scope="module")
def uniq():
    """Return a random uniq value"""
    return randomstr(length=8, lowercase=True)

def randomstr(length: int=16, lowercase: bool=False):
    """Generate a random string"""
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str(''.join(random.choices(letters + string.digits, k=length)))
