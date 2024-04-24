"""Test functions in es_testbed.TestBed"""
# pylint: disable=redefined-outer-name,missing-function-docstring,missing-class-docstring
import pytest
from es_testbed import TestPlan, TestBed
from es_testbed.helpers.es_api import get_ds_current

@pytest.fixture(scope='module')
def settings(prefix, uniq, repo):
    if not repo:
        pytest.skip('No snapshot repository', allow_module_level=True)
    return {
        'type': 'data_streams',
        'prefix': prefix,
        'uniq': uniq,
        'ilm': {
            'tiers': ['hot', 'cold', 'delete'],
            'forcemerge': False,
            'max_num_segments': 1,
            'repository': repo,
        }
    }

class TestFrozenDataStream:
    @pytest.fixture(scope="class")
    def tb(self, client, settings):
        teebee = TestBed(client, plan=TestPlan(settings=settings))
        teebee.setup()
        yield teebee
        teebee.teardown()

    def test_entity_count(self, tb):
        assert len(tb.tracker.entities.entity_list) == 1

    def test_name(self, tb, namecore):
        value = f'{namecore('data_stream')}'
        assert tb.tracker.entities.last == value

    def test_first_backing(self, cold, namecore, ymd, tb):
        idx = f'{cold}.ds-{namecore('data_stream')}-{ymd}-000001'
        assert tb.tracker.entities.indexlist[0] == idx

    def test_write_index(self, tb, namecore):
        ds = f'{namecore('data_stream')}'
        assert tb.tracker.entities.indexlist[-1] == get_ds_current(tb.client, ds)

    def test_index_template(self, tb, namecore):
        components = []
        components.append(f'{namecore('component')}-000001')
        components.append(f'{namecore('component')}-000002')
        assert tb.tracker.components.entity_list == components
        template = f'{namecore('template')}-000001'
        assert tb.tracker.templates.last == template
        result = tb.client.indices.get_index_template(name=template)['index_templates']
        assert len(result) == 1
        assert result[0]['index_template']['composed_of'] == components
