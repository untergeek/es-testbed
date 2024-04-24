"""Test functions in es_testbed.defaults"""
# pylint: disable=missing-function-docstring,redefined-outer-name
import pytest
from es_testbed.defaults import ilmcold, ilmwarm, ilm_force_merge, ilm_phase

@pytest.fixture
def forcemerge():
    def _forcemerge(mns: int=1):
        return {'forcemerge': {'max_num_segments': mns}}
    return _forcemerge

def test_default_ilm_fm(forcemerge):
    assert ilm_force_merge() == forcemerge()

def test_default_ilm_fm_mns(forcemerge):
    mns = 2
    assert ilm_force_merge(max_num_segments=mns) == forcemerge(mns=mns)

def test_default_ilm_warm():
    tier = 'warm'
    assert ilm_phase(tier) == {tier: ilmwarm()}

def test_default_ilm_cold():
    tier = 'cold'
    assert ilm_phase(tier) == {tier: ilmcold()}
