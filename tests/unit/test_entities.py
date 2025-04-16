"""Unit tests for es_testbed.entities module"""

# pylint: disable=C0115,C0116,R0903,R0913,R0917,W0212
from unittest.mock import MagicMock, patch
import typing as t
import re
import pytest
from elastic_transport import ApiResponseMeta
from elasticsearch8.exceptions import BadRequestError
from es_wait.exceptions import EsWaitFatal, EsWaitTimeout
from es_testbed.exceptions import TestbedFailure
from . import ALIAS, INDEX1, INDICES, my_retval


ALIAS_FIXT: str = 'alias,idx_list,retval,expected'
"""CSV string of pytest.mark.parametrize fixtures for Alias tests."""

ALIAS_PARAMS: dict = {
    'default_match': (ALIAS, INDICES, my_retval('alias_body', {}), True),
    'src_idx_nomatch': (
        ALIAS,
        ['missing1', 'missing2'],
        my_retval('alias_body', {}),
        False,
    ),
    'src_alias_nomatch': ('nomatch', INDICES, my_retval('alias_body', {}), False),
    'retval_alias_nomatch': (
        ALIAS,
        INDICES,
        my_retval('alias_body', {'alias': 'nomatch'}),
        False,
    ),
    'retval_idx_nomatch': (
        ALIAS,
        INDICES,
        my_retval('alias_body', {'indices': ['no1', 'no2']}),
        False,
    ),
}
"""Test parameters for Alias tests"""

INDEX_FIXT: str = 'phase,phases,expected,logmsg'
"""CSV string of pytest.mark.parametrize fixtures for Index tests."""

INDEX_PARAMS: dict = {
    'stay-hot': (
        'hot',
        ['hot', 'delete'],
        'hot',
        f'ILM Policy for "{INDEX1}" has no cold/frozen phases',
    ),
    'hot-cold-frozen2cold': ('hot', ['hot', 'cold', 'frozen'], 'cold', ''),
    'hot2cold': ('hot', ['hot', 'cold'], 'cold', ''),
    'hot2frozen': ('hot', ['hot', 'frozen'], 'frozen', ''),
    'hot-cold-frozen,cold2frozen': ('cold', ['hot', 'cold', 'frozen'], 'frozen', ''),
    'hot-cold-frozen,stay-frozen': ('frozen', ['hot', 'cold', 'frozen'], 'frozen', ''),
}
"""Test parameters for Index tests"""


def paramlist(test_map: dict) -> t.Sequence[pytest.param]:
    """Return the list of pytest.param objects from the test_map."""
    retval = []
    for test_id in test_map:
        retval.append(pytest.param(*test_map[test_id], id=test_id))
    return retval


# --- Alias Tests ---


@pytest.mark.parametrize(ALIAS_FIXT, paramlist(ALIAS_PARAMS), indirect=True)
def test_verify_method(alias, alias_cls, expected, idx_list, retval, caplog):
    """Test Alias.verify method."""
    alias_cls.set_debug_tier(5)
    with patch(
        'es_testbed.entities.alias.resolver',
        return_value={'aliases': [retval]},
    ):
        result = alias_cls.verify(idx_list)
        assert result is expected
        if result:
            assert 'Confirm list index position [0] match of alias' in caplog.text
            assert f'Confirm match of indices backed by alias {alias}' in caplog.text
    alias_cls.set_debug_tier(1)


# --- Index Tests ---


@pytest.mark.parametrize(INDEX_FIXT, paramlist(INDEX_PARAMS), indirect=True)
def test_get_target(index_cls, phase, phases, expected, logmsg, caplog):
    index_cls.set_debug_tier(3)
    index_cls.ilm_tracker.policy_phases = phases
    index_cls.ilm_tracker.explain.phase = phase
    index_cls.ilm_tracker.pname.side_effect = lambda x: {
        'hot': 1,
        'warm': 2,
        'cold': 3,
        'frozen': 4,
    }[x]
    target = index_cls._get_target
    assert target == expected
    if logmsg:
        assert logmsg in caplog.text
    index_cls.set_debug_tier(1)


def test_phase_tuple(index_cls):
    index_cls.ilm_tracker.explain.phase = 'hot'
    index_cls.ilm_tracker.policy_phases = ['hot', 'cold', 'frozen']
    index_cls.ilm_tracker.pname.side_effect = lambda x: {
        'hot': 1,
        'cold': 2,
        'frozen': 3,
    }[x]
    assert index_cls.phase_tuple == ('hot', 'cold')


def test_add_snap_step(index_cls, caplog):
    index_cls.set_debug_tier(5)
    index_cls.snapmgr = MagicMock()
    snap = 'snapshot1'
    with patch('es_testbed.entities.index.snapshot_name', return_value=snap):
        index_cls._add_snap_step()
        assert 'Getting snapshot name for tracking...' in caplog.text
        assert f'Snapshot {snap} backs {INDEX1}' in caplog.text
        index_cls.snapmgr.add_existing.assert_called_once_with(snap)
    index_cls.set_debug_tier(1)


def test_ilm_step(index_cls, caplog):
    caplog.set_level(10)
    phase, action, name = ('hot', 'complete', 'step1')
    index_cls.ilm_tracker.explain.phase = phase
    index_cls.ilm_tracker.explain.action = action
    index_cls.ilm_tracker.explain.step = name
    index_cls.client.ilm.explain_lifecycle.return_value = {
        "indices": {INDEX1: {'phase': phase, 'action': action, 'step': name}}
    }
    index_cls.set_debug_tier(5)
    with patch('es_testbed.entities.index.IlmStep') as mock_ilm_step:
        mock_ilm_step_instance = mock_ilm_step.return_value
        index_cls._ilm_step()
        assert f'DEBUG5 {INDEX1}: Current Step:' in caplog.text
        assert f"'phase': '{phase}'" in caplog.text
        assert f"'action': '{action}'" in caplog.text
        assert f"'name': '{name}'" in caplog.text
        mock_ilm_step_instance.wait.assert_called_once()
    index_cls.set_debug_tier(1)


def test_ilm_step_failure(index_cls):
    index_cls.ilm_tracker = MagicMock()
    phase, action, name = ('hot', 'complete', 'step1')
    index_cls.ilm_tracker.explain.phase = phase
    index_cls.ilm_tracker.explain.action = action
    index_cls.ilm_tracker.explain.step = name
    with patch('es_testbed.entities.index.logger.debug'):
        with patch('es_testbed.entities.index.IlmStep') as mock_ilm_step:
            mock_ilm_step_instance = mock_ilm_step.return_value
            with patch.object(
                index_cls, '_wait_try', side_effect=TestbedFailure('testbed failure')
            ) as mock_wait_try:
                with patch('es_testbed.entities.index.logger.error') as mock_error:
                    with pytest.raises(TestbedFailure):
                        index_cls._ilm_step()
                        mock_wait_try.assert_any_call(mock_ilm_step_instance.wait)
                        mock_error.assert_called_once_with('testbed failure')


def test_wait_try_success(index_cls):
    func = MagicMock()
    index_cls._wait_try(func)
    func.assert_called_once()


def test_wait_try_es_wait_fatal(index_cls):
    func = MagicMock(
        side_effect=EsWaitFatal('fatal error', elapsed=10, errors=['error1'])
    )
    with pytest.raises(TestbedFailure, match='fatal error. Elapsed time: 10. Errors:'):
        index_cls._wait_try(func)


def test_wait_try_es_wait_timeout(index_cls):
    func = MagicMock(side_effect=EsWaitTimeout('timeout error', 10.0, 10.0))
    with pytest.raises(TestbedFailure, match='timeout error. Total elapsed time: 10.'):
        index_cls._wait_try(func)


def test_wait_try_general_exception(index_cls):
    err = 'foo'
    msg = f"General Exception caught: \nException('{err}')"
    func = MagicMock(side_effect=Exception(err))
    with pytest.raises(TestbedFailure) as exc:
        index_cls._wait_try(func)
        assert re.match(exc.value.args[0], msg, re.MULTILINE)


def test_mounted_step(index_cls, caplog):
    caplog.set_level(10)
    index_cls.set_debug_tier(3)
    index_cls.ilm_tracker.advance = MagicMock()
    new = 'new-index'
    target = 'cold'
    index_cls.client.ilm.explain_lifecycle.return_value = {
        "indices": {new: {'phase': 'hot', 'action': 'complete', 'step': 'complete'}}
    }
    with patch('es_testbed.entities.index.mounted_name', return_value=new):
        with patch('es_testbed.entities.index.Exists') as mock_exists:
            mock_exists_instance = mock_exists.return_value
            index_cls._mounted_step(target)
            index_cls.ilm_tracker.advance.assert_called_once_with(phase=target)
            assert (
                f'Waiting for ILM phase change to complete. New index: {new}'
                in caplog.text
            )
            mock_exists_instance.wait.assert_called_once()
            assert f'Updating self.name from "{INDEX1}" to "{new}"...' in caplog.text
            assert 'Waiting for the ILM steps to complete...' in caplog.text
            assert f'Switching to track "{new}" as self.name...' in caplog.text
    index_cls.set_debug_tier(1)


def test_mounted_step_bad_request_error(index_cls):
    meta = ApiResponseMeta(404, '1.1', {}, 0.01, None)
    index_cls.ilm_tracker = MagicMock()
    index_cls.ilm_tracker.advance = MagicMock(
        side_effect=BadRequestError('bad request', meta, 'bad request')
    )
    with pytest.raises(BadRequestError, match='bad request'):
        index_cls._mounted_step('cold')


def test_mounted_step_testbed_failure(index_cls):
    new = 'new-index'
    index_cls.ilm_tracker = MagicMock()
    index_cls.ilm_tracker.advance = MagicMock()
    with patch('es_testbed.entities.index.mounted_name', return_value=new):
        with patch('es_testbed.entities.index.Exists') as mock_exists:
            mock_exists_instance = mock_exists.return_value
            with patch('es_testbed.entities.index.logger.debug'):
                with patch.object(
                    index_cls,
                    '_wait_try',
                    side_effect=TestbedFailure('testbed failure'),
                ) as mock_wait_try:
                    with patch('es_testbed.entities.index.logger.error') as mock_error:
                        with pytest.raises(TestbedFailure):
                            index_cls._mounted_step('cold')
                            mock_wait_try.assert_any_call(mock_exists_instance.wait)
                            mock_error.assert_called_once_with('testbed failure')


def test_manual_ss(index_cls):
    index_cls.snapmgr = MagicMock()
    new = 'new-index'
    target = 'cold'
    scheme = {'target_tier': target}
    with patch('es_testbed.entities.index.mounted_name', return_value=new):
        index_cls.manual_ss(scheme)
        assert index_cls.name == new


def test_mount_ss_no_policy(index_cls, caplog):
    caplog.set_level(10)
    index_cls.set_debug_tier(3)
    index_cls.snapmgr = MagicMock()
    scheme = {'target_tier': 'cold'}
    with patch.object(index_cls, 'manual_ss') as mock_manual_ss:
        index_cls.mount_ss(scheme)
        assert f'No ILM policy for "{INDEX1}". Trying manual...' in caplog.text
        mock_manual_ss.assert_called_once_with(scheme)
    index_cls.set_debug_tier(1)


def test_mount_ss_write_index(index_cls, caplog):
    caplog.set_level(10)
    index_cls.set_debug_tier(5)
    with patch('es_testbed.entities.Index.am_i_write_idx', return_value=True):
        scheme = {'target_tier': 'cold'}
        index_cls.mount_ss(scheme)
        assert (
            f'"{INDEX1}" is the write_index. Cannot mount as searchable snapshot'
            in caplog.text
        )
    index_cls.set_debug_tier(1)


def test_mount_ss_phase_new(index_cls, caplog):
    caplog.set_level(10)
    index_cls.set_debug_tier(3)
    curr, target = ('new', 'cold')
    index_cls.policy_name = 'test-policy'
    index_cls.ilm_tracker = MagicMock()
    index_cls.ilm_tracker.next_phase = target
    index_cls.ilm_tracker.explain.phase = curr
    scheme = {'target_tier': target}
    with patch('es_testbed.entities.index.IlmPhase') as mock_ilm_phase:
        mock_ilm_phase_instance = mock_ilm_phase.return_value
        with patch.object(index_cls, '_wait_try') as mock_wait_try:
            index_cls.mount_ss(scheme)
            assert (
                f'Our index is still in phase "{curr}"!. We need it to be in "{target}"'
                in caplog.text
            )
            mock_wait_try.assert_any_call(mock_ilm_phase_instance.wait)
    index_cls.set_debug_tier(1)


def test_mount_ss_phase_new_raises(index_cls, caplog):
    caplog.set_level(10)
    err = 'SPECIFIC ERROR'
    curr, target = ('new', 'cold')
    index_cls.policy_name = 'test-policy'
    index_cls.ilm_tracker = MagicMock()
    index_cls.ilm_tracker.next_phase = target
    index_cls.ilm_tracker.explain.phase = curr
    with patch.object(index_cls, '_wait_try', side_effect=TestbedFailure(err)):
        with pytest.raises(TestbedFailure):
            index_cls.mount_ss({'target_tier': target})
            assert err in caplog.text
