"""Test functions in es_testbed.utils"""

# pylint: disable=missing-function-docstring,redefined-outer-name
from unittest.mock import patch
import datetime
import pytest
from es_testbed.defaults import TIER
from es_testbed.exceptions import TestbedMisconfig
from es_testbed.utils import (
    build_ilm_phase,
    build_ilm_policy,
    get_routing,
    iso8601_now,
    python_version,
    mounted_name,
    raise_on_none,
    randomstr,
    storage_type,
    prettystr,
    process_preset,
)
from . import forcemerge


def test_build_ilm_phase_defaults(builtphase, tiers, tiertestval, trepo):
    for phase in tiers:
        assert builtphase(phase, repo=trepo[phase]) == tiertestval(
            phase, repo=trepo[phase]
        )


def test_build_ilm_phase_add_action():
    expected = {'read_only': {}}
    phase = 'warm'
    assert build_ilm_phase(phase, actions=expected)[phase]['actions'] == expected


def test_build_ilm_phase_fail_repo(builtphase):
    with pytest.raises(TestbedMisconfig):
        builtphase('cold', repo=None)


def test_build_ilm_policy_no_phase():
    expected = {
        'phases': {
            'hot': {
                'actions': {
                    'rollover': {'max_age': '1d', 'max_primary_shard_size': '1gb'}
                }
            },
            'delete': {
                'actions': {'delete': {}},
                'min_age': '5d',
            },
        }
    }
    assert build_ilm_policy() == expected


# This allows me to run multiple testing scenarios in the same test space
def test_build_ilm_policy(repo_val, tiertestval, trepo):
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
                tiertestval(tier, repo=trepo[tier], **kwargs)
            )  # Update with values per tier
        if fm:  # If we're doing forcemerge
            phases['hot']['actions'].update(
                forcemerge(fm=fm, mns=mns)
            )  # Update the hot tier
        # To keep the line more readable, build the kwargs as a dict first
        kwargs = {'repository': repo_val, 'forcemerge': fm, 'max_num_segments': mns}
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


def test_process_preset_builtin():
    modpath, tmpdir = process_preset(builtin='builtin', path=None, ref=None, url=None)
    assert modpath == 'es_testbed.presets.builtin'
    assert tmpdir is None


def test_process_preset_path():
    with patch('es_testbed.utils.Path') as mock_path:
        mock_path.return_value.resolve.return_value.is_dir.return_value = True
        mock_path.return_value.resolve.return_value.name = 'path'
        modpath, tmpdir = process_preset(
            builtin=None, path='some/path', ref=None, url=None
        )
        assert modpath == 'path'
        assert tmpdir is None


def test_process_preset_git():
    with patch('es_testbed.utils.Repo.clone_from') as mock_clone:
        with patch('es_testbed.utils.mkdtemp', return_value='/tmp/dir'):
            with patch('es_testbed.utils.Path') as mock_path:
                mock_path.return_value.resolve.return_value.is_dir.return_value = True
                mock_path.return_value.resolve.return_value.name = 'dir'
                modpath, tmpdir = process_preset(
                    builtin=None,
                    path='some/path',
                    ref='main',
                    url='https://github.com/repo.git',
                )
                assert modpath == 'dir'
                assert tmpdir == '/tmp/dir'
                mock_clone.assert_called_once_with(
                    'https://github.com/repo.git', '/tmp/dir', branch='main', depth=1
                )


def test_process_preset_invalid_path():
    with patch('es_testbed.utils.Path') as mock_path:
        mock_path.return_value.resolve.return_value.is_dir.return_value = False
        with pytest.raises(
            ValueError, match='The provided path "invalid/path" is not a directory'
        ):
            process_preset(builtin=None, path='invalid/path', ref=None, url=None)


def test_python_version():
    version = python_version()
    assert isinstance(version, tuple)
    assert len(version) == 3


def test_raise_on_none():
    with pytest.raises(ValueError, match='kwarg "key" cannot have a None value'):
        raise_on_none(key=None)


def test_randomstr():
    result = randomstr(length=8, lowercase=True)
    assert len(result) == 8
    assert result.islower()


def test_prettystr():
    data = {'key': 'value'}
    result = prettystr(data)
    assert isinstance(result, str)
    assert 'key' in result
    assert 'value' in result


def test_prettystr_with_kwargs():
    data = {'key': 'value'}
    result = prettystr(data, indent=4, width=100, sort_dicts=True)
    assert isinstance(result, str)
    assert 'key' in result
    assert 'value' in result


def test_build_ilm_phase_hot():
    """Test building the 'hot' phase without additional actions."""
    phase = 'hot'
    response = build_ilm_phase(phase)
    expected = {
        phase: {
            'actions': {'rollover': {'max_age': '1d', 'max_primary_shard_size': '1gb'}}
        }
    }
    assert response == expected


def test_build_ilm_phase_warm_with_actions():
    """Test building the 'warm' phase with additional actions."""
    actions = {"allocate": {"number_of_replicas": 1}}
    phase = build_ilm_phase('warm', actions=actions)
    assert phase == {
        'warm': {'actions': {"allocate": {"number_of_replicas": 1}}, 'min_age': '2d'}
    }


def test_build_ilm_phase_cold_with_repo():
    """Test building the 'cold' phase with a repository."""
    phase = build_ilm_phase('cold', repo='my-repo')
    assert phase == {
        'cold': {
            'actions': {
                'searchable_snapshot': {
                    'snapshot_repository': 'my-repo',
                    'force_merge_index': False,
                }
            },
            'min_age': '3d',
        }
    }


def test_build_ilm_phase_frozen_with_fm():
    """Test building the 'frozen' phase with force merge enabled."""
    phase = build_ilm_phase('frozen', repo='my-repo', fm=True)
    assert phase == {
        'frozen': {
            'actions': {
                'searchable_snapshot': {
                    'snapshot_repository': 'my-repo',
                    'force_merge_index': True,
                }
            },
            'min_age': '4d',
        }
    }


def test_build_ilm_phase_cold_without_repo():
    """Test that building 'cold' phase without a repository raises an exception."""
    with pytest.raises(TestbedMisconfig, match="Unable to build cold ILM phase"):
        build_ilm_phase('cold')


@pytest.mark.parametrize(
    'dt,tz,expected',
    [
        ((2023, 1, 1, 12, 0, 0), 'UTC', '2023-01-01T12:00:00Z'),
        ((2023, 1, 1, 12, 0, 0), 1, '2023-01-01T12:00:00+01:00'),
    ],
    indirect=True,
)
def test_iso8601_now(mock_datetime_now, fake_now, dt, tz, expected):
    """Test that iso8601_now returns a timestamp with the correct offset."""
    _ = mock_datetime_now
    assert datetime.datetime(*dt, tzinfo=tz) == fake_now
    assert iso8601_now() == expected


@pytest.mark.parametrize(
    'idx,tier,expected',
    [('idx1', 'cold', 'restored-idx1'), ('idx1', 'frozen', 'partial-idx1')],
    indirect=True,
)
def test_mounted_name(idx, tier, expected):
    """Test mounted_name for cold & frozen tiers."""
    assert mounted_name(idx, tier) == expected


@pytest.mark.parametrize('phase', ['cold', 'frozen'], indirect=True)
def test_storage_type(phase):
    """Test storage_type for tier."""
    assert storage_type(phase) == TIER[phase]['storage']


@patch('es_testbed.utils.TIER', {'hot': {'pref': 'data_hot'}})
def test_get_routing_known_tier():
    """Test routing for a known tier."""
    result = get_routing('hot')
    expected = {'index.routing.allocation.include._tier_preference': 'data_hot'}
    assert result == expected, "Should return preference from TIER for known tier"


@patch('es_testbed.utils.TIER', {})
def test_get_routing_unknown_tier():
    """Test routing for an unknown tier falls back to 'data_content'."""
    result = get_routing('unknown')
    expected = {'index.routing.allocation.include._tier_preference': 'data_content'}
    assert result == expected, "Should fall back to 'data_content' for unknown tier"


# Can't get these to work right now
#
# @patch('es_testbed.utils.mkdtemp')
# @patch('es_testbed.utils.Repo.clone_from')
# def test_process_preset_git_success(mock_clone_from, mock_mkdtemp):
#     """Test successful Git clone in process_preset."""
#     mock_mkdtemp.return_value = '/tmp/testdir'
#     mock_clone_from.return_value = MagicMock()  # Simulate successful clone
#     modpath, tmpdir = process_preset(
#         builtin=None, path='some/path', ref='main', url='http://example.com/repo.git'
#     )
#     assert tmpdir == '/tmp/testdir', "Temporary directory should be set"
#     assert modpath == 'path', "modpath should be the final directory name from path"
#
#
# @patch('es_testbed.utils.mkdtemp')
# @patch('es_testbed.utils.rmtree')
# @patch('es_testbed.utils.logger')
# def test_process_preset_git_failure(mock_logger, mock_rmtree, mock_mkdtemp):
#     """Test Git clone failure in process_preset."""
#     mock_mkdtemp.return_value = '/tmp/testdir'
#     with patch(
#         'es_testbed.helpers.utils.process_preset.repo.clone_from',
#         side_effect=Exception('Clone failed'),
#     ):
#         with pytest.raises(Exception, match='Clone failed'):
#             process_preset(
#                 builtin=None,
#                 path='/some/path',
#                 ref='main',
#                 url='http://example.com/repo.git',
#             )
#     mock_logger.error.assert_called_once_with('Git clone failed: Clone failed')
#     mock_rmtree.assert_called_once_with(
#         '/tmp/testdir'
#     ), "Should clean up tmpdir on failure"
