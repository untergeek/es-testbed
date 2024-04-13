"""TestPlan Class Definition"""
from es_testbed.classes.ilmbuilder import IlmBuilder
from es_testbed.defaults import TESTPLAN
from es_testbed.helpers.utils import randomstr
# pylint: disable=missing-docstring

class TestPlan:
    def __init__(self, planbook: dict=None):
        self.type = TESTPLAN['indices']
        self.prefix = TESTPLAN['prefix']
        self.rollover_alias = TESTPLAN['rollover_alias']
        self.uniq = randomstr(length=8, lowercase=True)
        self.entities = []
        self.ilm = IlmBuilder()
        self.planbook = planbook
        self.bookbuild()
        ### Example planbook
        # planbook={
        #   'type': 'indices', # Default is indices? Or should it be datastreams?
        #   'prefix': 'es-testbed', # Provide this value as a default
        #   'rollover_alias': True, # Only respected if 'type' == 'indices'.
        #                         # Will rollover after creation and filling
        #   'uniq': 'my-unique-str', # If not provided, randomstr(length=8, lowercase=True)
        #   'ilm': { # All of these ILM values are defaults
        #     'tiers': ['hot', 'delete'],
        #     'forcemerge': False,
        #     'max_num_segments': 1,
        #     'repository': None,
        #   }
        #
        #   # If these keys aren't specified per entity, then all entities will get this treatment
        #   # EXCEPT for the is_write_index for aliases and datastreams
        #
        #   'defaults': {
        #     'entity_count': 3,
        #     'docs': 10,
        #     'match': True,
        #     'searchable': tier...
        #   }

        #   # Manually specifying entities makes sense for individual indices, but not so much for
        #   # alias-backed indices or data_streams
        #   'entities': [
        #    {
        #      'docs': 10,
        #      'match': True,
        #      'searchable': 'frozen'
        #    },
        #    {
        #      'docs': 10,
        #      'match': False,
        #      'searchable': 'cold',
        #    },
        #    {
        #      'docs': 10,
        #      'match': True,
        #      'searchable': 'frozen'
        #    },
        #   ]
        # }
    def add_entity(
            self,
            docs: int=10,
            match: bool=True,
            searchable: str=None
        ):
        entity = {'docs': docs, 'match': match}
        if searchable:
            entity['searchable'] = searchable
        self.entities.append(entity)

    def bookbuild(self):
        if self.planbook:
            autokeys = ['type', 'prefix', 'uniq', 'rollover_alias', 'entities']
            for dkey in autokeys:
                if dkey in self.planbook:
                    setattr(self, dkey, self.planbook[dkey])
            if not self.entities:
                self.make_default_entities()
            self.update_ilm()

    def make_default_entities(self):
        defs = TESTPLAN['defaults']
        if 'defaults' in self.planbook and self.planbook['defaults']:
            defs = self.planbook['defaults']
        kwargs = {'docs': defs['docs'], 'match': defs['match'], 'searchable': defs['searchable']}
        for _ in range(0, defs['entity_count']):
            self.add_entity(**kwargs)

    def update_ilm(self):
        if 'ilm' in self.planbook:
            for k,v in self.planbook.items():
                setattr(self.ilm, k, v)
        # If cold or frozen tiers weren't included in planbook['tiers'], we manually correct here
        for entity in self.entities:
            if 'searchable' in entity and entity['searchable'] is not None:
                if not entity['searchable'] in self.ilm.tiers:
                    self.ilm.tiers.append(entity['searchable'])

