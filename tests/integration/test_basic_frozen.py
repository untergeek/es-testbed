"""Test functions in es_testbed.TestBed"""
# pylint: disable=redefined-outer-name,missing-function-docstring,missing-class-docstring
import pytest
from es_testbed import TestPlan, TestBed

@pytest.fixture(scope='module')
def settings(prefix, uniq, repo):
    if not repo:
        pytest.skip('No snapshot repository', allow_module_level=True)
    return {
        'type': 'indices',
        'prefix': prefix,
        'rollover_alias': False,
        'repository': repo,
        'uniq': uniq,
        'ilm': False,
        'defaults': {
            'entity_count': 3,
            'docs': 10,
            'match': True,
            'searchable': 'frozen',
        }
    }

class TestManualFrozenIndices:
    @pytest.fixture(scope="class")
    def tb(self, client, settings):
        teebee = TestBed(client, plan=TestPlan(settings=settings))
        teebee.setup()
        yield teebee
        teebee.teardown()

    def test_entity_count(self, tb):
        assert len(tb.tracker.entities.entity_list) == 3

    def test_first_index(self, tb, frozen, namecore):
        value = f'{frozen}{namecore('index')}-000001'
        assert tb.tracker.entities.entity_list[0].name == value

    def test_last_index(self, tb, frozen, namecore):
        value = f'{frozen}{namecore('index')}-000003'
        assert tb.tracker.entities.last.name == value

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
