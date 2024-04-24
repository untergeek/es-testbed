"""Top-level conftest.py"""
# pylint: disable=missing-function-docstring,redefined-outer-name
from os import environ
from datetime import datetime, timezone
import random
import string
import pytest
from elasticsearch8.exceptions import NotFoundError
from es_client import Builder
from es_client.helpers.logging import set_logging
from es_testbed.defaults import NAMEMAPPER

LOGLEVEL = 'DEBUG'

@pytest.fixture(scope='session')
def client():
    """Return an Elasticsearch client"""
    host = environ.get('TEST_ES_SERVER', 'http://127.0.0.1:9200')
    file = environ.get('ES_CLIENT_FILE', None) # Path to es_client YAML config
    if file:
        kwargs = {'configfile': file}
    else:
        kwargs = {'configdict': {'elasticsearch': {'client': {'hosts': host}}}}
    set_logging({'loglevel': LOGLEVEL, 'blacklist': ['elastic_transport', 'urllib3']})
    builder = Builder(**kwargs)
    builder.connect()
    return builder.client

@pytest.fixture(scope='module')
def cold():
    """Return the prefix for cold indices"""
    return 'restored-'

@pytest.fixture(scope='module')
def ymd():
    return datetime.now(timezone.utc).strftime('%Y.%m.%d')

@pytest.fixture(scope='module')
def frozen():
    """Return the prefix for frozen indices"""
    return 'partial-'

@pytest.fixture(scope='module')
def namecore(prefix, uniq):
    def _namecore(kind):
        return f'{prefix}-{NAMEMAPPER[kind]}-{uniq}'
    return _namecore

@pytest.fixture(scope='module')
def prefix():
    """Return a random prefix"""
    return randomstr(length=8, lowercase=True)

def randomstr(length: int=16, lowercase: bool=False):
    """Generate a random string"""
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str(''.join(random.choices(letters + string.digits, k=length)))

@pytest.fixture(scope='module')
def repo(client):
    """Return the elasticsearch repository"""
    name = environ.get('TEST_ES_REPO', 'found-snapshots') # Going with Cloud default
    if not repo:
        return False
    try:
        client.snapshot.get_repository(name=name)
    except NotFoundError:
        return False
    return name # Return the repo name if it's online

@pytest.fixture(scope='module')
def uniq():
    """Return a random uniq value"""
    return randomstr(length=8, lowercase=True)
