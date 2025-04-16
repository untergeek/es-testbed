"""Unit tests for the ILM Tracker module"""

# pylint: disable=C0115,C0116,R0903,R0913,R0917,W0212
from os import getenv
from unittest.mock import MagicMock, patch
import logging
import pytest
from dotmap import DotMap
from es_wait.exceptions import EsWaitTimeout, EsWaitFatal
from es_testbed.debug import debug
from es_testbed.defaults import (
    PAUSE_DEFAULT,
    PAUSE_ENVVAR,
    TIMEOUT_DEFAULT,
    TIMEOUT_ENVVAR,
)
from es_testbed.exceptions import TestbedFailure
from es_testbed.ilm import IlmTracker
from es_testbed.exceptions import ResultNotExpected, TestbedMisconfig, NameChanged
from . import INDEX1

# Constants assumed from the module
PAUSE_VALUE = float(getenv(PAUSE_ENVVAR, default=PAUSE_DEFAULT))
TIMEOUT_VALUE = float(getenv(TIMEOUT_ENVVAR, default=TIMEOUT_DEFAULT))

RESOLVER = {
    'indices': [{'name': INDEX1}],
    'aliases': [],
    'data_streams': [],
}


# Test Initialization
@patch('es_testbed.ilm.resolver')
@patch('es_testbed.ilm.ilm_explain')
@patch('es_testbed.ilm.get_ilm_phases')
def test_init(mock_get_ilm_phases, mock_ilm_explain, mock_resolver, client):
    mock_resolver.return_value = RESOLVER
    mock_ilm_explain.return_value = {
        'phase': 'hot',
        'action': 'complete',
        'step': 'complete',
    }
    mock_get_ilm_phases.return_value = {'hot': {}, 'warm': {}, 'delete': {}}

    tracker = IlmTracker(client, INDEX1)

    assert tracker.name == INDEX1
    assert tracker._explain.phase == 'hot'
    assert tracker._phases == {'hot': {}, 'warm': {}, 'delete': {}}


# Test Properties
def test_current_step(tracker):
    tracker._explain = DotMap(
        {'phase': 'hot', 'action': 'rollover', 'step': 'check-rollover'}
    )
    with patch.object(tracker, 'update'):  # Mock update to avoid external call
        assert tracker.current_step() == {
            'phase': 'hot',
            'action': 'rollover',
            'name': 'check-rollover',
        }


def test_explain(tracker):
    tracker._explain = DotMap({'phase': 'hot'})
    assert tracker.explain == DotMap({'phase': 'hot'})


def test_next_phase(tracker):
    tracker._explain = DotMap({'phase': 'hot'})
    tracker._phases = {'hot': {}, 'warm': {}, 'delete': {}}
    assert tracker.next_phase() == 'warm'

    tracker._explain.phase = 'warm'
    assert tracker.next_phase() == 'delete'

    tracker._explain.phase = 'delete'
    assert tracker.next_phase() is None


def test_policy_phases(tracker):
    tracker._phases = {'hot': {}, 'warm': {}, 'delete': {}}
    assert tracker.policy_phases == ['hot', 'warm', 'delete']


# Test Advance Method
@patch('es_testbed.ilm.IlmPhase')
@patch('es_testbed.ilm.IlmStep')
@patch('es_testbed.ilm.ilm_explain')
def test_advance_to_warm(mock_ilm_explain, mock_ilm_step, mock_ilm_phase, client):
    mock_ilm_explain.side_effect = [
        {'phase': 'hot', 'action': 'complete', 'step': 'complete'},
        {'phase': 'warm', 'action': 'incomplete', 'step': 'incomplete'},
        {'phase': 'warm', 'action': 'complete', 'step': 'complete'},
    ]
    with patch(
        'es_testbed.ilm.get_ilm_phases',
        return_value={'hot': {}, 'warm': {}, 'delete': {}},
    ):
        with patch('es_testbed.ilm.resolver', return_value=RESOLVER):
            tracker = IlmTracker(client, INDEX1)

    mock_phase_instance = MagicMock()
    mock_ilm_phase.return_value = mock_phase_instance
    mock_step_instance = MagicMock()
    mock_ilm_step.return_value = mock_step_instance

    tracker.advance(phase='warm')

    assert tracker.current_step() == {
        'phase': 'warm',
        'action': 'complete',
        'name': 'complete',
    }


# Test Next Step
def test_next_step(tracker):
    tracker._phases = {'hot': {}, 'warm': {}, 'delete': {}}

    assert tracker.next_step(phase='warm') == {'phase': 'warm'}
    assert tracker.next_step(phase='warm', action='some_action', name='some_step') == {
        'phase': 'warm',
        'action': 'some_action',
        'name': 'some_step',
    }

    with pytest.raises(TestbedMisconfig):
        tracker.next_step(phase='warm', action='some_action')

    with pytest.raises(TestbedMisconfig):
        tracker.next_step(phase='warm', name='some_step')


# Test Phase Number and Name Mappings
def test_pnum_pname(tracker):
    assert tracker.pnum('new') == 0
    assert tracker.pnum('hot') == 1
    assert tracker.pnum('warm') == 2
    assert tracker.pnum('cold') == 3
    assert tracker.pnum('frozen') == 4
    assert tracker.pnum('delete') == 5

    assert tracker.pname(0) == 'new'
    assert tracker.pname(1) == 'hot'
    assert tracker.pname(2) == 'warm'
    assert tracker.pname(3) == 'cold'
    assert tracker.pname(4) == 'frozen'
    assert tracker.pname(5) == 'delete'


# Test Resolve
@patch('es_testbed.ilm.resolver')
def test_resolve(mock_resolver, client):

    mock_resolver.return_value = {
        'indices': [{'name': INDEX1}],
        'aliases': [],
        'data_streams': [],
    }
    with patch('es_testbed.ilm.ilm_explain', return_value={'phase': 'hot'}):
        with patch('es_testbed.ilm.get_ilm_phases', return_value={'hot': {}}):
            assert IlmTracker(client, INDEX1).name == INDEX1

    mock_resolver.return_value = {
        'indices': [],
        'aliases': ['test_alias'],
        'data_streams': [],
    }
    with pytest.raises(ResultNotExpected):
        IlmTracker(client, 'test_alias')

    mock_resolver.return_value = {
        'indices': [],
        'aliases': [],
        'data_streams': ['test_ds'],
    }
    with pytest.raises(ResultNotExpected):
        IlmTracker(client, 'test_ds')

    mock_resolver.return_value = {
        'indices': [{'name': 'index1'}, {'name': 'index2'}],
        'aliases': [],
        'data_streams': [],
    }
    with pytest.raises(ResultNotExpected):
        IlmTracker(client, 'index*')


# Test Update
@patch('es_testbed.ilm.ilm_explain')
def test_update(mock_ilm_explain, tracker):
    mock_ilm_explain.return_value = {
        'phase': 'warm',
        'action': 'complete',
        'step': 'complete',
    }
    tracker._explain = DotMap({'phase': 'hot'})

    tracker.update()

    assert tracker._explain.phase == 'warm'


@patch('es_testbed.ilm.ilm_explain')
def test_update_name_changed(mock_ilm_explain, tracker):
    mock_ilm_explain.side_effect = NameChanged('Name changed')

    with pytest.raises(NameChanged):
        tracker.update()


# Test Wait for Complete
@patch('es_testbed.ilm.IlmStep')
def test_wait4complete(mock_ilm_step, tracker):
    tracker._explain = DotMap(
        {'phase': 'hot', 'action': 'complete', 'step': 'complete'}
    )
    tracker.wait4complete()
    mock_ilm_step.assert_not_called()

    tracker._explain = DotMap(
        {'phase': 'warm', 'action': 'incomplete', 'step': 'incomplete'}
    )
    mock_step_instance = MagicMock()
    mock_ilm_step.return_value = mock_step_instance

    tracker.wait4complete()

    mock_ilm_step.assert_called_once_with(
        tracker.client, name=INDEX1, pause=PAUSE_VALUE, timeout=TIMEOUT_VALUE
    )
    mock_step_instance.wait.assert_called_once()


# Test Get Explain Data
@patch('es_testbed.ilm.ilm_explain')
def test_get_explain_data(mock_ilm_explain, tracker):
    mock_ilm_explain.return_value = {'phase': 'hot'}
    assert tracker.get_explain_data() == {'phase': 'hot'}


@patch('es_testbed.ilm.ilm_explain')
def test_get_explain_data_name_changed(mock_ilm_explain, tracker):
    mock_ilm_explain.side_effect = NameChanged('Name changed')
    with pytest.raises(NameChanged):
        tracker.get_explain_data()


@patch('es_testbed.ilm.ilm_explain')
def test_get_explain_data_raises(mock_ilm_explain, tracker):
    mock_ilm_explain.side_effect = ResultNotExpected('error')
    with pytest.raises(ResultNotExpected):
        tracker.get_explain_data()


# Test successful phase wait
@patch('es_testbed.ilm.IlmPhase')
def test_phase_wait_success(mock_ilm_phase, tracker):
    """Test _phase_wait when the phase is reached successfully."""
    mock_phase_instance = MagicMock()
    mock_ilm_phase.return_value = mock_phase_instance
    mock_phase_instance.wait.return_value = None  # Simulate successful wait

    tracker._phase_wait('warm')

    mock_ilm_phase.assert_called_once_with(
        tracker.client,
        name=INDEX1,
        phase='warm',
        pause=PAUSE_VALUE,
        timeout=TIMEOUT_VALUE,
    )
    mock_phase_instance.wait.assert_called_once()


# Test phase wait with timeout
@patch('es_testbed.ilm.IlmPhase')
def test_phase_wait_timeout(mock_ilm_phase, tracker):
    """Test _phase_wait when a timeout occurs."""
    mock_phase_instance = MagicMock()
    mock_ilm_phase.return_value = mock_phase_instance
    mock_phase_instance.wait.side_effect = EsWaitTimeout(
        'Timeout waiting for phase', 30.5, TIMEOUT_VALUE
    )

    with pytest.raises(TestbedFailure) as exc_info:
        tracker._phase_wait('warm')

    assert 'Timeout waiting for phase' in str(exc_info.value)
    mock_ilm_phase.assert_called_once_with(
        tracker.client,
        name=INDEX1,
        phase='warm',
        pause=PAUSE_VALUE,
        timeout=TIMEOUT_VALUE,
    )
    mock_phase_instance.wait.assert_called_once()


# Test phase wait with fatal error
@patch('es_testbed.ilm.IlmPhase')
def test_phase_wait_fatal_error(mock_ilm_phase, tracker):
    """Test _phase_wait when a fatal error occurs."""
    mock_phase_instance = MagicMock()
    mock_ilm_phase.return_value = mock_phase_instance
    mock_phase_instance.wait.side_effect = EsWaitFatal('Fatal error during wait', 30.5)

    with pytest.raises(TestbedFailure) as exc_info:
        tracker._phase_wait('warm')

    assert 'Fatal error during wait' in str(exc_info.value)
    mock_ilm_phase.assert_called_once_with(
        tracker.client,
        name=INDEX1,
        phase='warm',
        pause=PAUSE_VALUE,
        timeout=TIMEOUT_VALUE,
    )
    mock_phase_instance.wait.assert_called_once()


# Test wait4complete when the step is already complete
def test_wait4complete_already_complete(tracker):
    """Test wait4complete when the ILM step is already complete."""
    tracker._explain = MagicMock()
    tracker._explain.action = 'complete'
    tracker._explain.step = 'complete'
    with patch('es_testbed.ilm.IlmStep') as mock_ilm_step:
        tracker.wait4complete()
        mock_ilm_step.assert_not_called()


# Test wait4complete when the step is not complete and wait succeeds
@patch('es_testbed.ilm.IlmStep')
def test_wait4complete_not_complete_success(mock_ilm_step, tracker):
    """Test wait4complete when the step is not complete and the wait succeeds."""
    tracker._explain = MagicMock()
    tracker._explain.action = 'incomplete'
    tracker._explain.step = 'incomplete'
    mock_step_instance = MagicMock()
    mock_ilm_step.return_value = mock_step_instance
    mock_step_instance.wait.return_value = None  # Simulate successful wait

    tracker.wait4complete()

    mock_ilm_step.assert_called_once_with(
        tracker.client, name=INDEX1, pause=PAUSE_VALUE, timeout=TIMEOUT_VALUE
    )
    mock_step_instance.wait.assert_called_once()


# Test wait4complete when the step is not complete and wait times out
@patch('es_testbed.ilm.IlmStep')
def test_wait4complete_not_complete_timeout(mock_ilm_step, tracker):
    """Test wait4complete when the step is not complete and the wait times out."""
    tracker._explain = MagicMock()
    tracker._explain.action = 'some_action'
    tracker._explain.step = 'some_step'
    mock_step_instance = MagicMock()
    mock_ilm_step.return_value = mock_step_instance
    mock_step_instance.wait.side_effect = EsWaitTimeout(
        'Timeout waiting for step', 30.5, TIMEOUT_VALUE
    )

    with pytest.raises(TestbedFailure) as exc_info:
        tracker.wait4complete()

    assert 'Timeout waiting for step' in str(exc_info.value)
    mock_ilm_step.assert_called_once_with(
        tracker.client, name=INDEX1, pause=PAUSE_VALUE, timeout=TIMEOUT_VALUE
    )
    mock_step_instance.wait.assert_called_once()


# Test wait4complete when the step is not complete and wait encounters a fatal error
@patch('es_testbed.ilm.IlmStep')
def test_wait4complete_not_complete_fatal_error(mock_ilm_step, tracker):
    """Test wait4complete when the step is not complete and a fatal error occurs."""
    tracker._explain = MagicMock()
    tracker._explain.action = 'some_action'
    tracker._explain.step = 'some_step'
    mock_step_instance = MagicMock()
    mock_ilm_step.return_value = mock_step_instance
    mock_step_instance.wait.side_effect = EsWaitFatal(
        'Fatal error during wait', TIMEOUT_VALUE
    )

    with pytest.raises(TestbedFailure) as exc_info:
        tracker.wait4complete()

    assert 'Fatal error during wait' in str(exc_info.value)
    mock_ilm_step.assert_called_once_with(
        tracker.client, name=INDEX1, pause=PAUSE_VALUE, timeout=TIMEOUT_VALUE
    )
    mock_step_instance.wait.assert_called_once()


def test_advance_already_on_delete_phase(tracker_adv):
    """
    Test that advance does nothing and logs a warning when already on 'delete' phase.
    """
    tracker_adv._explain.phase = "delete"

    with patch('es_testbed.es_api.ilm_move') as mock_ilm_move, patch.object(
        tracker_adv, "_phase_wait"
    ) as mock_phase_wait, patch.object(
        tracker_adv, "wait4complete"
    ) as mock_wait4complete, patch(
        'es_testbed.ilm.debug.lv1'
    ) as mock_debug:

        tracker_adv.advance(phase="warm")

        # Assertions
        mock_ilm_move.assert_not_called()
        mock_phase_wait.assert_not_called()
        mock_wait4complete.assert_not_called()
        mock_debug.assert_called_once_with(
            'Already on "delete" phase. No more phases to advance'
        )


def test_advance_from_new_to_hot(tracker_adv):
    """Test advancing from 'new' to 'hot' phase, only waiting for the phase."""
    tracker_adv._explain.phase = "new"
    expected = 'hot'
    with patch('es_testbed.es_api.ilm_move') as mock_ilm_move, patch.object(
        tracker_adv, "_phase_wait"
    ) as mock_phase_wait, patch.object(
        tracker_adv, "wait4complete"
    ) as mock_wait4complete, patch(
        'es_testbed.ilm.debug.lv2'
    ) as mock_debug:

        tracker_adv.advance(phase=expected)

        # Assertions
        mock_phase_wait.assert_called_once_with(expected)
        mock_ilm_move.assert_not_called()
        mock_wait4complete.assert_called_once()
        mock_debug.assert_called_with(f'Index "{INDEX1}" now on phase "{expected}"')


def test_advance_beyond_hot_to_warm(tracker_adv, caplog):
    """Test advancing from 'hot' to 'warm' phase with step completion."""
    caplog.set_level(logging.DEBUG)
    start, end = 'hot', 'warm'
    with debug.change_level(3):
        tracker_adv._explain.phase = start
        with patch('es_testbed.es_api.ilm_move'), patch(
            'es_testbed.ilm.IlmTracker.next_phase', return_value=end
        ), patch.object(tracker_adv, "update"), patch.object(
            tracker_adv, "wait4complete"
        ), patch.object(
            tracker_adv, "_phase_wait"
        ):

            assert tracker_adv.current_step()['phase'] == start
            tracker_adv.advance(phase=end)
            assert 'DEBUG3 Advancing to "warm" phase' in caplog.text


def test_advance_already_on_target_phase(tracker_adv, caplog):
    """Test that advance does nothing when already on the target phase."""
    caplog.set_level(logging.DEBUG)
    expected = 'warm'
    step = {'phase': expected, 'action': 'complete', 'name': 'complete'}
    with debug.change_level(3):
        tracker_adv._explain.phase = expected
        tracker_adv._explain.action = 'complete'
        tracker_adv._explain.step = 'complete'

        with patch('es_testbed.es_api.ilm_move') as mock_ilm_move, patch(
            'es_testbed.ilm.ilm_explain',
            return_value={'phase': expected, 'action': 'complete', 'step': 'complete'},
        ), patch(
            'es_testbed.ilm.IlmTracker.current_step', return_value=step
        ), patch.object(
            tracker_adv, 'next_phase', return_value=expected
        ), patch.object(
            tracker_adv, "_phase_wait"
        ) as mock_phase_wait, patch.object(
            tracker_adv, "wait4complete"
        ) as mock_wait4complete:

            tracker_adv.advance(phase=expected)

            # Assertions
            mock_ilm_move.assert_not_called()
            mock_phase_wait.assert_not_called()
            mock_wait4complete.assert_called_once()
            assert (
                f'DEBUG3 Already on "{expected}" phase. No need to advance'
                in caplog.text
            )


def test_advance_phase_wait_error_handling(tracker_adv, caplog):
    """Test that exceptions from _phase_wait are propagated."""
    caplog.set_level(logging.DEBUG)
    tracker_adv._explain.phase = "hot"

    with patch('es_testbed.es_api.ilm_move'), patch.object(
        tracker_adv, "_phase_wait"
    ) as mock_phase_wait, patch.object(
        tracker_adv, "wait4complete"
    ) as mock_wait4complete:

        mock_phase_wait.side_effect = TestbedFailure("Phase wait failed")

        with pytest.raises(TestbedFailure, match="Phase wait failed"):
            tracker_adv.advance(phase="warm")

        # Assertions
        mock_phase_wait.assert_called_once_with("warm")
        mock_wait4complete.assert_called_once()  # Called before moving
        assert "Advancing to phase: warm" not in caplog.text


def test_next_step_no_phase(tracker):
    """Test next_step when no phase is provided."""
    with patch.object(tracker, 'next_phase', return_value='warm'):
        assert tracker.next_step(phase=None) == {'phase': 'warm'}
