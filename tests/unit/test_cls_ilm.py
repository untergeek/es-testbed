"""Test functions in es_testbed.defaults"""
# pylint: disable=missing-function-docstring,redefined-outer-name
import pytest
from es_testbed.defaults import TESTPLAN
from es_testbed.classes.ilm import IlmBuilder

@pytest.fixture
def ilm():
    yield IlmBuilder()

@pytest.fixture
def hot():
    return {'actions': {
                'rollover': {'max_age': '1d', 'max_primary_shard_size': '1gb'}}}

@pytest.fixture
def cold():
    return {'actions': {
                'searchable_snapshot': {'snapshot_repository': 'repo'}}, 'min_age': '3d'}

@pytest.fixture
def delete():
    return {'actions': {'delete': {}}, 'min_age': '5d'}

@pytest.fixture
def phases():
    def _phases(**kwargs):
        retval = {'phases':{}}
        for key in ['hot', 'warm', 'cold', 'frozen', 'delete']:
            if key in kwargs and kwargs[key] is not None:
                retval['phases'][key] = kwargs[key]
        if 'fm' in kwargs and kwargs['fm'] is True:
            mns = kwargs['mns'] if 'mns' in kwargs else 1
            retval['phases']['hot']['actions']['forcemerge'] = {'max_num_segments': mns}
        return retval
    return _phases

def test_cls_ilm_defaults(ilm):
    for key in ['tiers', 'forcemerge', 'max_num_segments', 'repository']:
        assert getattr(ilm, key) == TESTPLAN['ilm'][key]

def test_cls_ilm_defaults_policy(ilm, hot, delete, phases):
    # assert ilm.policy == {'phases': {'hot': hot, 'delete': delete}}
    assert ilm.policy == phases(hot=hot, delete=delete)

def test_cls_ilm_forcemerge(ilm, hot, delete, phases):
    ilm.forcemerge = True
    # assert ilm.policy == {'phases': {'hot': forcemerge(mns=1), 'delete': delete}}
    assert ilm.policy == phases(hot=hot, delete=delete, fm=True)

def test_cls_ilm_mns(ilm, hot, delete, phases):
    val = 2
    ilm.forcemerge = True
    ilm.max_num_segments = val
    assert ilm.policy == phases(hot=hot, delete=delete, fm=True, mns=2)

def test_cls_ilm_cold(ilm, hot, cold):
    expected = {'phases': {'hot': hot, 'cold': cold}}
    ilm.tiers = ['hot', 'cold']
    ilm.repository = 'repo'
    assert ilm.policy['phases'].keys() == expected['phases'].keys()
