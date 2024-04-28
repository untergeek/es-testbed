"""Test functions in es_testbed.helpers.utils"""

# pylint: disable=missing-function-docstring,redefined-outer-name
import typing as t
import pytest
from dotmap import DotMap
from es_testbed.defaults import (
    ilmhot,
    ilmwarm,
    ilmcold,
    ilmfrozen,
    ilmdelete,
)
from es_testbed.exceptions import TestbedMisconfig
from es_testbed.helpers.utils import build_ilm_phase, build_ilm_policy, doc_gen

FMAP: t.Dict[str, t.Dict] = {
    'hot': ilmhot(),
    'warm': ilmwarm(),
    'cold': ilmcold(),
    'frozen': ilmfrozen(),
    'delete': ilmdelete(),
}
REPO: str = 'repo'
TIERS: t.Sequence[str] = ['hot', 'warm', 'cold', 'frozen', 'delete']
TREPO: t.Dict[str, t.Union[str, None]] = {
    'hot': None,
    'warm': None,
    'cold': REPO,
    'frozen': REPO,
    'delete': None,
}


def searchable(repo: str = None) -> t.Union[t.Dict[str, t.Dict[str, str]], None]:
    if repo:
        return {'searchable_snapshot': {'snapshot_repository': repo}}
    return {}


def forcemerge(
    fm: bool = False, mns: int = 1
) -> t.Union[t.Dict[str, t.Dict[str, str]], None]:
    if fm:
        return {'forcemerge': {'max_num_segments': mns}}
    return {}


@pytest.fixture
def tiertestval():
    def _tiertestval(tier: str, repo: str = None, fm: bool = False, mns: int = 1):
        retval = {tier: FMAP[tier]}
        retval[tier]['actions'].update(searchable(repo))
        retval[tier]['actions'].update(forcemerge(fm=fm, mns=mns))
        return retval

    return _tiertestval


@pytest.fixture
def builtphase():
    def _builtphase(tier: str, repo: str = None, fm: bool = False, mns: int = 1):
        return build_ilm_phase(
            tier, actions=forcemerge(fm=fm, mns=mns), repository=repo
        )

    return _builtphase


def test_build_ilm_phase_defaults(builtphase, tiertestval):
    for tier in TIERS:
        assert builtphase(tier, repo=TREPO[tier]) == tiertestval(tier, repo=TREPO[tier])


def test_build_ilm_phase_add_action():
    expected = {'foo': 'bar'}
    tier = 'warm'
    assert build_ilm_phase(tier, actions=expected)[tier]['actions'] == expected


def test_build_ilm_phase_fail_repo(builtphase):
    with pytest.raises(TestbedMisconfig):
        builtphase('cold', repo=None)


# This allows me to run multiple testing scenarios in the same test space
def test_build_ilm_policy(tiertestval):
    # 3 tests building different ILM policies with different tiers
    tgroups = [
        ['hot', 'delete'],
        ['hot', 'frozen', 'delete'],
        ['hot', 'cold', 'delete'],
    ]
    # Each tier group corresponds to a forcemerge plan by list index, with each index a
    # tuple for forcemerge True/False and max_num_segment count
    fmerge = [(False, 0), (False, 0), (True, 3)]
    for idx, tgrp in enumerate(tgroups):  # Iterate over testing scenarios
        phases = {}  # Build out the phase dict for each scenario
        fm, mns = fmerge[idx]  # Extract whether to use forcemerge by index/tuple
        for tier in tgrp:  # Iterate over each tier in the testing scenario
            phases.update(
                tiertestval(tier, repo=TREPO[tier])
            )  # Update with values per tier
        if fm:  # If we're doing forcemerge
            phases['hot']['actions'].update(
                forcemerge(fm=fm, mns=mns)
            )  # Update the hot tier
        # To keep the line more readable, build the kwargs as a dict first
        kwargs = {'repository': REPO, 'forcemerge': fm, 'max_num_segments': mns}
        # Then pass it as **kwargs
        assert build_ilm_policy(tgrp, **kwargs) == {'phases': phases}
        # Our policy is easier to build at the last minute rather than constantly
        # passing dict['phases']['tier']


def test_build_ilm_policy_fail_repo():
    with pytest.raises(TestbedMisconfig):
        build_ilm_policy(['hot', 'frozen'], repository=None)


@pytest.fixture
def fieldmatch():
    def _fieldmatch(val: str, num: int):
        return f'{val}{num}'

    return _fieldmatch


def test_doc_gen_matching(fieldmatch):
    i = 0
    for res in doc_gen(count=3, start_at=0, match=True):
        doc = DotMap(res)
        tests = [
            (doc.message, 'message'),
            (doc.nested.key, 'nested'),
            (doc.deep.l1.l2.l3, 'deep'),
        ]
        for test in tests:
            dm, val = test
            assert dm == fieldmatch(val, i)
        i += 1
