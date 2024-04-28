"""Integration Test Setup"""

import logging
import pytest
from es_testbed import PlanBuilder, TestBed


class TestAny:
    """
    Test Any index or data_stream

    Set this up by setting class variables, as below.
    """

    sstier = 'hot'
    kind = 'data_stream'
    roll = False
    repo_test = False
    ilm = {
        'enabled': False,
        'tiers': ['hot', 'delete'],
        'forcemerge': False,
        'max_num_segments': 1,
    }
    logger = logging.getLogger(__name__)

    @pytest.fixture(scope="class")
    def tb(self, client, settings, skip_no_repo):
        """TestBed setup/teardown"""
        skip_no_repo(self.repo_test)
        cfg = settings(
            plan_type=self.kind,
            rollover_alias=self.roll,
            ilm=self.ilm,
            sstier=self.sstier,
        )
        theplan = PlanBuilder(settings=cfg).plan
        teebee = TestBed(client, plan=theplan)
        teebee.setup()
        yield teebee
        teebee.teardown()

    def test_entity_count(self, entity_count, entitymgr, tb):
        """Count the number of entities (index or data_stream)"""
        assert len(entitymgr(tb).entity_list) == entity_count(self.kind)

    def test_name(self, actual_rollover, rollovername, tb):
        """
        Verify the name of a rollover alias or data_stream

        Will still return True if not a rollover or data_stream enabled test
        """
        assert actual_rollover(tb) == rollovername(tb.plan)

    def test_first_index(self, actual_index, first, index_name, tb):
        """Assert that the first index matches the expected name"""
        expected = index_name(which=first, plan=tb.plan, tier=self.sstier)
        actual = actual_index(tb, first)
        assert actual == expected

    def test_last_index(self, actual_index, index_name, last, tb):
        """Assert that the last index matches the expected name"""
        expected = index_name(which=last, plan=tb.plan, tier=self.sstier)
        actual = actual_index(tb, last)
        assert actual == expected

    def test_write_index(self, tb, actual_write_index, write_index_name):
        """
        Test that the write index or current data_stream target is correct

        Will still return True if not a rollover or data_stream enabled test
        """
        expected = write_index_name(plan=tb.plan, tier=self.sstier)
        self.logger.debug('PLAN: %s', tb.plan)
        self.logger.debug('expected: %s', expected)
        actual = actual_write_index(tb)
        self.logger.debug('actual: %s', actual)
        assert actual == expected

    def test_index_template(self, tb, components, get_template, template):
        """Assert that the index template and component templates are correct"""
        assert tb.componentmgr.entity_list == components
        assert tb.templatemgr.last == template
        result = get_template(tb.client)
        assert len(result) == 1
        assert result[0]['index_template']['composed_of'] == components
