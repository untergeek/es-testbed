"""Hot tier tests. No ILM. No searchable snapshots."""

from . import TestAny

SSTIER = 'hot'


class TestDataStream(TestAny):
    """TestDataStream"""

    sstier = SSTIER
    kind = 'data_stream'
    roll = False
    repo_test = False
    ilm = {'enabled': False}


class TestIndices(TestAny):
    """TestIndices"""

    sstier = SSTIER
    kind = 'indices'
    roll = False
    repo_test = False
    ilm = {'enabled': False}


class TestRolloverIndices(TestAny):
    """TestRolloverIndices"""

    sstier = SSTIER
    kind = 'indices'
    roll = True
    repo_test = False
    ilm = {'enabled': False}
