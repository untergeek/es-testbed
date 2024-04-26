"""TestPlan Class Definition"""

import typing as t
from dotmap import DotMap
from es_testbed.defaults import TESTPLAN
from es_testbed.helpers.utils import build_ilm_policy, getlogger, randomstr

# pylint: disable=missing-docstring


class PlanBuilder:

    def __init__(self, settings: t.Dict = None, default_entities: bool = True, autobuild: t.Optional[bool] = True):
        self.logger = getlogger('es_testbed.PlanBuilder')
        self.default_entities = default_entities
        if settings is None:
            settings = TESTPLAN
        self.settings = settings
        self._plan = DotMap(TESTPLAN)
        if autobuild:
            self.setup()

        # ## Example settings
        # settings={
        #   'type': 'indices',       # Default is indices? Or should it be data_streams?
        #   'prefix': 'es-testbed',  # Provide this value as a default
        #   'rollover_alias': False, # Only respected if 'type' == 'indices'.
        #                            # Will rollover after creation and filling 1st index
        #                            # If True, will be overridden to value of alias name
        #                            # If False, will be overridden with None
        #   'uniq': 'my-unique-str', # If not provided, randomstr(length=8, lowercase=True)
        #   'repository':            # Only used for cold/frozen tier for snapshots
        #   'ilm': {                 # All of these ILM values are defaults
        #     'enabled': False,
        #     'tiers': ['hot', 'delete'],
        #     'forcemerge': False,
        #     'max_num_segments': 1,
        #   }
        #
        # # If these keys aren't specified per entity, then all entities will get this treatment
        # # EXCEPT for the is_write_index for aliases and data_streams
        #
        #   'defaults': {
        #     'entity_count': 3,
        #     'docs': 10,
        #     'match': True,
        #     'searchable': tier...
        #   }
        #
        # # Manually specifying entities makes sense for individual indices, but not so much for
        # # alias-backed indices or data_streams
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
        #      'searchable': 'hot'
        #    },
        #   ]
        # }

    @property
    def plan(self):
        return self._plan

    def _create_failsafes(self):
        self._plan.failsafes = DotMap()
        items = ['index', 'data_stream', 'snapshot', 'ilm', 'template', 'component', 'entity_type']
        for i in items:
            self._plan.failsafes[i] = []

    def _create_lists(self):
        names = [
            'indices',
            'data_stream',
            'snapshots',
            'ilm_policies',
            'index_templates',
            'component_templates',
            'entity_mgrs',
        ]
        for name in names:
            self._plan[name] = []

    def add_entity(
        self, docs: t.Optional[int] = 10, match: t.Optional[bool] = True, searchable: t.Optional[str] = None
    ) -> None:
        entity = {'docs': docs, 'match': match}
        if searchable:
            entity['searchable'] = searchable
        self._plan.entities.append(entity)

    def make_default_entities(self) -> None:
        defs = TESTPLAN['defaults']  # Start with defaults
        if 'defaults' in self._plan:
            defs = self._plan.defaults
        kwargs = {'docs': defs['docs'], 'match': defs['match'], 'searchable': defs['searchable']}
        for _ in range(0, defs['entity_count']):
            self.add_entity(**kwargs)
        self.logger.debug('Plan will create %s (backing) indices', len(self._plan.entities))

    def setup(self) -> None:
        self._plan.uniq = randomstr(length=8, lowercase=True)
        self._create_lists()
        self._create_failsafes()
        self.update(self.settings)  # Override with settings.
        self.update_rollover_alias()
        self.update_ilm()
        if not self._plan.entities:
            if self.default_entities:
                self.make_default_entities()
        self.logger.debug('Test Plan: %s', self._plan.pprint())

    def update(self, settings: t.Dict) -> None:
        self._plan.update(**settings)

    def update_ilm(self) -> None:
        setdefault = False
        if 'ilm' not in self._plan:
            self.logger.debug('key "ilm" is not in plan')
            setdefault = True
        if isinstance(self._plan.ilm, dict):
            _ = DotMap(self._plan.ilm)
            self._plan.ilm = _
        if isinstance(self._plan.ilm, DotMap):
            if 'enabled' not in self._plan.ilm:
                # Override with defaults
                self.logger.debug('plan.ilm does not have key "enabled". Overriding with defaults')
                setdefault = True
        elif isinstance(self._plan.ilm, bool):
            if self._plan.ilm:
                self.logger.warning('"plan.ilm: True" is incorrect. Use plan.ilm.enabled: True')
            self.logger.debug('plan.ilm is boolean. Overriding with defaults')
            setdefault = True
        if setdefault:
            self.logger.debug('Setting defaults for ILM')
            self._plan.ilm = DotMap(TESTPLAN['ilm'])
        ilm = self._plan.ilm
        for entity in self._plan.entities:
            if 'searchable' in entity and entity['searchable'] is not None:
                if not entity['searchable'] in ilm.tiers:
                    ilm.tiers.append(entity['searchable'])
        kwargs = {
            'tiers': ilm.tiers,
            'forcemerge': ilm.forcemerge,
            'max_num_segments': ilm.max_num_segments,
            'repository': self._plan.repository,
        }
        self._plan.ilm.policy = build_ilm_policy(**kwargs)

    def update_rollover_alias(self) -> None:
        if self._plan.rollover_alias:
            self._plan.rollover_alias = f'{self._plan.prefix}-idx-{self._plan.uniq}'
        else:
            self._plan.rollover_alias = None
