"""Unit tests for the base module."""

from unittest.mock import MagicMock, patch
import pytest
from es_testbed._base import TestBed
from es_testbed.defaults import NAMEMAPPER
from es_testbed.exceptions import ResultNotExpected

# pylint: disable=W0212


def test_init(testbed):
    """Test TestBed initialization."""
    assert testbed.client is not None
    assert testbed.settings is not None


def test_init_no_preset():
    """Test TestBed initialization without a preset."""
    with patch('es_testbed._base.process_preset', return_value=(None, None)):
        with pytest.raises(ValueError, match='Must define a preset'):
            TestBed(
                client=MagicMock(),
                builtin='searchable_test',
                path='path',
                ref='ref',
                url='url',
                scenario='hot',
            )


def test_init_import_error():
    """Test TestBed initialization with an import error."""
    with patch('es_testbed._base.process_preset', return_value=('modpath', None)):
        with patch('es_testbed._base.import_module', side_effect=ImportError):
            with pytest.raises(ImportError):
                TestBed(
                    client=MagicMock(),
                    builtin='searchable_test',
                    path='path',
                    ref='ref',
                    url='url',
                    scenario='hot',
                )


def test_init_import_process_preset_tmpdir(testbed):
    """Test TestBed initialization with a tmpdir."""
    expected = '/tmp/dir'
    with patch(
        'es_testbed._base.process_preset',
        return_value=('es_testbed.presets.searchable_test', expected),
    ):
        testbed = TestBed(
            client=MagicMock(),
            builtin='searchable_test',
            path='some/path',
            ref='main',
            url='https://github.com/repo.git',
        )
        # pylint: disable=E1136
        assert getattr(testbed, 'settings')['tmpdir'] == expected


def test_erase(testbed):
    """Test erase method."""
    with patch('es_testbed._base.delete', return_value=True):
        assert testbed._erase('index', ['test-index']) is True


def test_erase_ilm(testbed):
    """Test erase method with ILM."""
    with patch('es_testbed._base.delete', return_value=True):
        assert testbed._erase('ilm', ['test-ilm']) is True


def test_erase_failure(testbed):
    """Test erase method failure."""
    with patch('es_testbed._base.delete', return_value=False):
        assert testbed._erase('index', ['test-index']) is False


# def test_fodder_generator(testbed):
#     """Test fodder generator."""
#     with patch('es_testbed._base.get', return_value=['test-index']):
#         generator = testbed._fodder_generator()
#         items = list(generator)
#         assert len(items) == 6


def test_fodder_generator(testbed_fodder):
    """Test fodder generator for each kind with multiple entities"""
    with patch('es_testbed._base.get', return_value=['entity1', 'entity2']) as mock_get:
        generator = testbed_fodder._fodder_generator()
        items = list(generator)
        assert len(items) == 6
        for kind, entities in items:
            assert kind in [
                'index',
                'data_stream',
                'snapshot',
                'template',
                'component',
                'ilm',
            ]
            assert entities == ['entity1', 'entity2']
            pattern = (
                f'*{testbed_fodder.plan.prefix}-{NAMEMAPPER[kind]}-'
                f'{testbed_fodder.plan.uniq}*'
            )
            mock_get.assert_any_call(
                testbed_fodder.client,
                kind,
                pattern,
                repository=testbed_fodder.plan.repository,
            )


def test_fodder_generator_no_repository(testbed_fodder):
    """
    Test fodder generator when no repository is set with each kind
    with multiple entities
    """
    testbed_fodder.plan.repository = None
    with patch('es_testbed._base.get', return_value=['entity1', 'entity2']) as mock_get:
        with patch('es_testbed._base.debug.lv4') as mock_debug:
            generator = testbed_fodder._fodder_generator()
            items = list(generator)
            assert len(items) == 5  # 'snapshot' should be skipped
            for kind, entities in items:
                assert kind in ['index', 'data_stream', 'template', 'component', 'ilm']
                assert entities == ['entity1', 'entity2']
                pattern = (
                    f'*{testbed_fodder.plan.prefix}-{NAMEMAPPER[kind]}-'
                    f'{testbed_fodder.plan.uniq}*'
                )
                mock_get.assert_any_call(
                    testbed_fodder.client,
                    kind,
                    pattern,
                    repository=testbed_fodder.plan.repository,
                )
            # assert 'No repository, no snapshots.' in caplog.text
            mock_debug.assert_called_with('No repository, no snapshots.')


def test_while(testbed):
    """Test while method."""
    with patch('es_testbed._base.delete', return_value=True):
        assert testbed._while('index', 'test-index') is True


def test_while_failure(testbed):
    """Test while method failure."""
    with patch('es_testbed._base.delete', side_effect=ResultNotExpected('error')):
        assert testbed._while('index', 'test-index') is False


def test_get_ilm_polling_produces_debug_log(testbed, caplog):
    """Test get_ilm_polling produces debug log"""
    testbed.set_debug_tier(5)
    testbed.get_ilm_polling()
    assert 'Cluster settings' in caplog.text
    testbed.set_debug_tier(1)


def test_get_ilm_polling_exception(testbed, caplog):
    """
    Test get_ilm_polling method when an exception is raised getting the cluster
    settings
    """
    caplog.set_level(40)  # CRITICAL
    with patch.object(
        testbed.client.cluster, 'get_settings', side_effect=Exception('error')
    ):
        with pytest.raises(Exception, match='error'):
            testbed.get_ilm_polling()
            assert 'Unable to get persistent cluster settings' in caplog.text
            assert 'This could be permissions, or something larger' in caplog.text
            assert 'Exiting.' in caplog.text


def test_get_ilm_polling_1s(testbed):
    """Test ilm_polling method when"""
    assert testbed.ilm_polling('1s') == {'indices.lifecycle.poll_interval': '1s'}


def test_get_ilm_polling_already_set(testbed):
    """Test get_ilm_polling method when ILM polling is already set."""
    with patch.object(
        testbed.client.cluster,
        'get_settings',
        return_value={
            'persistent': {'indices': {'lifecycle': {'poll_interval': '1s'}}}
        },
    ):
        with patch('es_testbed._base.logger.warning') as mock_warning:
            testbed.get_ilm_polling()
            assert testbed.plan.ilm_polling_interval is None
            mock_warning.assert_called_once_with(
                'ILM polling already set at 1s. A previous run most likely did not '
                'tear down properly. Resetting to null after this run'
            )


def test_setup(testbed):
    """Test setup method."""
    with patch(
        'es_testbed._base.PlanBuilder', return_value=MagicMock(plan=MagicMock())
    ):
        with patch('es_testbed._base.TestBed.get_ilm_polling'):
            with patch('es_testbed._base.TestBed.setup_entitymgrs'):
                testbed.setup()
                assert testbed.plan is not None


def test_teardown(testbed):
    """Test teardown method."""
    with patch(
        'es_testbed._base.TestBed._fodder_generator',
        return_value=[('index', ['test-index'])],
    ):
        with patch('es_testbed._base.TestBed._erase', return_value=True):
            with patch('es_testbed._base.rmtree'):
                with patch(
                    'es_testbed._base.TestBed.ilm_polling',
                    return_value={'indices.lifecycle.poll_interval': '1s'},
                ):
                    testbed.teardown()
                    assert testbed.plan.cleanup is True


def test_teardown_not_successful(testbed):
    """Test teardown method when cleanup is not successful."""
    with patch(
        'es_testbed._base.TestBed._fodder_generator',
        return_value=[('index', ['test-index'])],
    ):
        with patch('es_testbed._base.TestBed._erase', return_value=False):
            with patch('es_testbed._base.rmtree'):
                with patch(
                    'es_testbed._base.TestBed.ilm_polling',
                    return_value={'indices.lifecycle.poll_interval': '1s'},
                ):
                    testbed.teardown()
                    assert testbed.plan.cleanup is False
                    assert testbed.plan.cleanup_error is not None
