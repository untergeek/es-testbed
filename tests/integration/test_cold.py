"""Cold tier tests. Mixed ILM"""

from . import TestAny

SSTIER = 'cold'
REPOTEST = True
ILM = {
    'enabled': True,
    'tiers': ['hot', SSTIER, 'delete'],
    'forcemerge': False,
    'max_num_segments': 1,
}


class TestDataStream(TestAny):
    """TestDataStream"""

    sstier = SSTIER
    kind = 'data_stream'
    roll = False
    repo_test = REPOTEST
    ilm = ILM


class TestIndices(TestAny):
    """TestIndices"""

    sstier = SSTIER
    kind = 'indices'
    roll = False
    repo_test = REPOTEST
    ilm = {'enabled': False}


class TestRolloverIndices(TestAny):
    """TestRolloverIndices"""

    sstier = SSTIER
    kind = 'indices'
    roll = True
    repo_test = REPOTEST
    ilm = {'enabled': False}


class TestRolloverIndicesILM(TestAny):
    """TestRolloverIndicesILM"""

    sstier = SSTIER
    kind = 'indices'
    roll = True
    repo_test = REPOTEST
    ilm = ILM
