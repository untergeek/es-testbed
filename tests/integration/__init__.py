"""Integration Test Setup"""

import logging
import pytest
from es_testbed import TestBed
from es_testbed.presets.searchable_test.definitions import get_plan

logger = logging.getLogger(__name__)

# pylint: disable=R0913


def get_kind(scenario) -> str:
    """Return the searchable snapshot tier for the test based on plan"""
    return get_plan(scenario=scenario)['type']


def get_sstier(scenario) -> str:
    """Return the searchable snapshot tier for the test based on plan"""
    plan = get_plan(scenario=scenario)
    tiers = set()
    for scheme in plan['index_buildlist']:
        if 'target_tier' in scheme:
            if scheme['target_tier'] in ['cold', 'frozen']:
                tiers.add(scheme['target_tier'])
    if 'ilm' in plan:
        if 'phases' in plan['ilm']:
            for phase in ['cold', 'frozen']:
                if phase in plan['ilm']['phases']:
                    tiers.add(phase)
    if len(tiers) > 1:
        raise ValueError('Both cold and frozen tiers specified for this scenario!')
    if tiers:
        retval = list(tiers)[0]  # There can be only one...
    else:
        retval = 'hot'
    return retval


class TestAny:
    """
    Test Any index or data_stream

    Set this up by setting class variables, as below.
    """

    scenario = None

    @pytest.fixture(scope="class")
    def tb(self, client, prefix, uniq, skip_no_repo):  # skip_localhost fixed?
        """TestBed setup/teardown"""
        skip_no_repo(get_sstier(self.scenario) in ['cold', 'frozen'])
        teebee = TestBed(client, builtin='searchable_test', scenario=self.scenario)
        teebee.settings['prefix'] = prefix
        teebee.settings['uniq'] = uniq
        teebee.setup()
        yield teebee
        teebee.teardown()

    def test_entity_count(self, entity_count, entitymgr, tb):
        """Count the number of entities (index or data_stream)"""
        assert len(entitymgr(tb).entity_list) == entity_count(get_kind(self.scenario))

    def test_name(self, actual_rollover, rollovername, tb):
        """
        Verify the name of a rollover alias or data_stream

        Will still return True if not a rollover or data_stream enabled test
        """
        expected = rollovername(tb.plan)
        actual = actual_rollover(tb)
        logger.debug('expected = %s', expected)
        logger.debug('actual = %s', actual)
        assert actual == expected

    def test_first_index(self, actual_index, first, index_name, tb):
        """Assert that the first index matches the expected name"""
        expected = index_name(which=first, plan=tb.plan, tier=get_sstier(self.scenario))
        actual = actual_index(tb, first)
        logger.debug('expected = %s', expected)
        logger.debug('actual = %s', actual)
        assert actual == expected

    def test_last_index(self, actual_index, index_name, last, tb):
        """Assert that the last index matches the expected name"""
        expected = index_name(which=last, plan=tb.plan, tier=get_sstier(self.scenario))
        actual = actual_index(tb, last)
        logger.debug('expected = %s', expected)
        logger.debug('actual = %s', actual)
        assert actual == expected

    def test_write_index(self, tb, actual_write_index, write_index_name):
        """
        Test that the write index or current data_stream target is correct

        Will still return True if not a rollover or data_stream enabled test
        """
        expected = write_index_name(plan=tb.plan, tier=get_sstier(self.scenario))
        actual = actual_write_index(tb)
        logger.debug('expected = %s', expected)
        logger.debug('actual = %s', actual)
        assert actual == expected

    def test_index_template(self, tb, components, get_template, template):
        """Assert that the index template and component templates are correct"""
        assert tb.componentmgr.entity_list == components
        assert tb.templatemgr.last == template
        result = get_template(tb.client)
        assert len(result) == 1
        assert result[0]['index_template']['composed_of'] == components
