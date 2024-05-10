"""Test functions in es_testbed.helpers.utils"""

# pylint: disable=missing-function-docstring,redefined-outer-name
import typing as t
import pytest
from es_testbed.defaults import (
    ilmhot,
    ilmwarm,
    ilmcold,
    ilmfrozen,
    ilmdelete,
)
from es_testbed.exceptions import TestbedMisconfig
from es_testbed.helpers.utils import build_ilm_phase, build_ilm_policy

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


def searchable(
    repo: str = None, fm: bool = None
) -> t.Union[t.Dict[str, t.Dict[str, str]], None]:
    if repo:
        return {
            'searchable_snapshot': {
                'snapshot_repository': repo,
                'force_merge_index': fm,
            }
        }
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
        retval[tier]['actions'].update(searchable(repo=repo, fm=fm))
        kwargs = {'fm': fm, 'mns': mns} if tier in ['hot', 'warm'] else {}
        retval[tier]['actions'].update(forcemerge(**kwargs))
        return retval

    return _tiertestval


@pytest.fixture
def builtphase():
    def _builtphase(tier: str, repo: str = None, fm: bool = False, mns: int = 1):
        return build_ilm_phase(tier, actions=forcemerge(fm=fm, mns=mns), repo=repo)

    return _builtphase


def test_build_ilm_phase_defaults(builtphase, tiertestval):
    for phase in TIERS:
        assert builtphase(phase, repo=TREPO[phase]) == tiertestval(
            phase, repo=TREPO[phase]
        )


def test_build_ilm_phase_add_action():
    expected = {'read_only': {}}
    phase = 'warm'
    assert build_ilm_phase(phase, actions=expected)[phase]['actions'] == expected


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
            kwargs = {'fm': fm} if tier in ['cold', 'frozen'] else {}
            phases.update(
                tiertestval(tier, repo=TREPO[tier], **kwargs)
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


def test_build_ilm_policy_read_only():
    expected = {'warm': {'min_age': '2d', 'actions': {'readonly': {}}}}
    # To keep the line more readable, build the kwargs as a dict first
    kwargs = {'readonly': 'warm'}
    # Then pass it as **kwargs
    assert build_ilm_policy(['warm'], **kwargs) == {'phases': expected}


def test_build_ilm_policy_fail_repo():
    with pytest.raises(TestbedMisconfig):
        build_ilm_policy(['hot', 'frozen'], repository=None)
