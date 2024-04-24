"""TestPlan Class Definition"""
import typing as t
from es_testbed.exceptions import TestPlanMisconfig
from es_testbed.defaults import TESTPLAN
from es_testbed.helpers.utils import randomstr, getlogger
from .args import Args
from .ilm import IlmBuilder
# pylint: disable=missing-docstring

class TestPlan(Args):
    __test__ = False
    def __init__(
            self,
            settings: t.Dict[str, t.Any] = None,
            defaults: t.Dict[str, t.Any] = None,
            default_entities: bool = True,
        ):
        if defaults is None:
            defaults = TESTPLAN
        super().__init__(settings=settings, defaults=defaults)
        self.logger = getlogger('es_testbed.TestPlan')
        self.entities = []
        self.ilm = False
        self.prefix = None
        self.uniq = randomstr(length=8, lowercase=True)
        self.repository = None
        self.update_settings(settings)
        self.logger.debug('settings = %s', self.asdict)
        self.update_ilm()
        if not self.entities:
            if default_entities:
                self.make_default_entities()

        ### Example settings
        # settings={
        #   'type': 'indices', # Default is indices? Or should it be data_streams?
        #   'prefix': 'es-testbed', # Provide this value as a default
        #   'rollover_alias': True, # Only respected if 'type' == 'indices'.
        #                         # Will rollover after creation and filling
        #   'uniq': 'my-unique-str', # If not provided, randomstr(length=8, lowercase=True)
        #   'repository': Only used for snapshots when ILM is not used.
        #   'ilm': { # All of these ILM values are defaults
        #     'tiers': ['hot', 'delete'],
        #     'forcemerge': False,
        #     'max_num_segments': 1,
        #     'repository': None,
        #   }
        #
        #   # If these keys aren't specified per entity, then all entities will get this treatment
        #   # EXCEPT for the is_write_index for aliases and data_streams
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
            docs: t.Optional[int] = 10,
            match: t.Optional[bool] = True,
            searchable: t.Optional[str] = None
        ) -> None:
        entity = {'docs': docs, 'match': match}
        if searchable:
            entity['searchable'] = searchable
        self.entities.append(entity)

    def make_default_entities(self) -> None:
        if self.settings and isinstance(self.settings, dict):
            if 'defaults' in self.settings:
                defs = self.settings['defaults']
        kwargs = {'docs': defs['docs'], 'match': defs['match'], 'searchable': defs['searchable']}
        for _ in range(0, defs['entity_count']):
            self.add_entity(**kwargs)
        self.logger.debug('Created %s entities', len(self.entities))

    def update_ilm(self) -> None:
        if self.use_ilm():
            self.ilm = IlmBuilder() # Set defaults
            for k,v in self.settings['ilm'].items():
                self.logger.debug('IlmBuilder.%s = %s', k, v)
                setattr(self.ilm, k, v)
            # If cold or frozen tiers weren't included in settings['ilm']['tiers']
            # we manually correct here
            for entity in self.entities:
                if 'searchable' in entity and entity['searchable'] is not None:
                    if not entity['searchable'] in self.ilm.tiers:
                        self.ilm.tiers.append(entity['searchable'])
        self.logger.debug('ILM settings = %s',
            self.ilm.asdict if isinstance(self.ilm, IlmBuilder) else self.ilm)


    def use_ilm(self) -> bool:
        use_ilm = False
        self.logger.debug('INIT: use_ilm = %s', use_ilm)
        if 'ilm' in self.settings:
            if isinstance(self.settings['ilm'], dict): # It's a dictionary, not a bool
                self.logger.debug('settings["ilm"] is a dictionary')
                use_ilm = True
            elif isinstance(self.settings['ilm'], bool):
                self.logger.debug('settings["ilm"] is a bool')
                use_ilm = self.settings['ilm'] # Accept whatever the boolean value is
            elif self.settings['ilm'] is None: # Empty dict is truthy, but is not None
                self.logger.debug('settings["ilm"] is None')
                use_ilm = False
        if self.entities:
            # Detect if cold or frozen tiers were included in settings['ilm']['tiers']
            for entity in self.entities:
                if 'searchable' in entity and entity['searchable'] is not None:
                    self.logger.debug('Test entities contain searchable snapshots')
                    if not use_ilm:
                        raise TestPlanMisconfig(
                            'Searchable entities were found, but ILM is disabled')
        self.logger.debug('FINAL: use_ilm = %s', use_ilm)
        return use_ilm
