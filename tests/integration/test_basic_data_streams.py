"""Test functions in es_testbed.TestBed"""
# pylint: disable=redefined-outer-name,missing-function-docstring,missing-class-docstring
import pytest
from es_testbed import TestPlan, TestBed
from es_testbed.defaults import NAMEMAPPER
from es_testbed.helpers.es_api import get_ds_current

@pytest.fixture(scope='module')
def settings(prefix, uniq):
    return {
        'type': 'data_streams',
        'prefix': prefix,
        'rollover_alias': False,
        'uniq': uniq,
        'ilm': {}, # use defaults
    }

class TestBasicDataStreams:
    @pytest.fixture(scope="class")
    def tb(self, client, settings):
        teebee = TestBed(client, plan=TestPlan(settings=settings))
        teebee.setup()
        yield teebee
        teebee.teardown()

    def test_entity_count(self, tb):
        assert len(tb.tracker.entities.entity_list) == 1

    def test_name(self, tb, prefix, uniq):
        value = f'{prefix}-{NAMEMAPPER['data_stream']}-{uniq}'
        assert tb.tracker.entities.last == value

    def test_write_index(self, tb, prefix, uniq):
        ds = f'{prefix}-{NAMEMAPPER['data_stream']}-{uniq}'
        assert tb.tracker.entities.indexlist[-1] == get_ds_current(tb.client, ds)

    def test_index_template(self, tb, prefix, uniq):
        components = []
        components.append(f'{prefix}-{NAMEMAPPER['component']}-{uniq}-000001')
        components.append(f'{prefix}-{NAMEMAPPER['component']}-{uniq}-000002')
        assert tb.tracker.components.entity_list == components
        template = f'{prefix}-{NAMEMAPPER['template']}-{uniq}-000001'
        assert tb.tracker.templates.last == template
        result = tb.client.indices.get_index_template(name=template)['index_templates']
        assert len(result) == 1
        assert result[0]['index_template']['composed_of'] == components
