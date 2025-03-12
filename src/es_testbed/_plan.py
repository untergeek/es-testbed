"""TestPlan Class Definition"""

import typing as t
import logging
from dotmap import DotMap
from es_testbed.defaults import TESTPLAN
from es_testbed.helpers.utils import build_ilm_policy, prettystr, randomstr

logger = logging.getLogger(__name__)


class PlanBuilder:
    """Plan builder class"""

    def __init__(
        self,
        settings: t.Dict = None,
        autobuild: t.Optional[bool] = True,
    ):
        if settings is None:
            raise ValueError('Must provide a settings dictionary')
        self.settings = settings
        logger.debug(f'SETTINGS: {settings}')
        self._plan = DotMap(TESTPLAN)
        logger.debug(f'INITIAL PLAN: {prettystr(self._plan)}')
        self._plan.cleanup = 'UNSET'  # Future use?
        if autobuild:
            self.setup()

        # ## Example settings
        # {
        #   'type': 'indices',       # Default is indices? Or should it be data_streams?
        #   'prefix': 'es-testbed',  # The default prefix for everything we create
        #   'rollover_alias': False, # Only respected if 'type' == 'indices'.
        #                            # Will rollover after creation and filling 1st
        #                            # If True, will be overridden to value of alias
        #                            # If False, will be overridden with None
        #   'uniq': 'my-unique-str', # If not provided, randomstr()
        #   'repository':            # Only used for cold/frozen tier for snapshots
        #   'ilm': {                 # All of these ILM values are defaults
        #     'enabled': False,
        #     'phases': ['hot', 'delete'],
        #     'readonly': PHASE      # Define readonly action during named PHASE
        #     'forcemerge': False,
        #     'max_num_segments': 1,
        #     'policy': {}           # Define full ILM policy in advance.
        #   }
        #

        #
        # # The index_buildlist
        # # The array of indices to build. Needs at least a single element. Importing
        # # is expected, but it can be modified or replaced after importing.
        #   'index_buildlist': [
        #    {
        #      'preset': 'NAME',         # docgen_preset name, included or otherwise
        #      'options': {              # kwargs for the generator function
        #        'docs': 10,
        #        'start_at': 0,
        #        'match': True,
        #      }
        #      'target_tier': 'frozen'   # Target tier for 1st (oldest) index created
        #    },
        #    {
        #      'preset': 'NAME',         # docgen_preset name, included or otherwise
        #      'options': {              # kwargs for the generator function
        #        'docs': 10,
        #        'start_at': 10,
        #        'match': True,
        #      }
        #      'target_tier': 'cold'     # Target tier for 2nd index created
        #    },
        #    {
        #      'preset': 'NAME',         # docgen_preset name, included or otherwise
        #      'options': {              # kwargs for the generator function
        #        'docs': 10,
        #        'start_at': 20,
        #        'match': True,
        #      }
        #      'target_tier': 'hot'      # Target tier for last (newest) index created
        #    },
        #   ]
        # }

    @property
    def plan(self) -> DotMap:
        """Return the Plan"""
        return self._plan

    def _create_lists(self) -> None:
        names = [
            'indices',
            'data_stream',
            'snapshots',
            'ilm_policies',
            'index_templates',
            'component_templates',
        ]
        for name in names:
            self._plan[name] = []

    def setup(self) -> None:
        """Do initial setup of the Plan DotMap"""
        self._plan.uniq = randomstr(length=8, lowercase=True)
        self._create_lists()
        self.update(self.settings)  # Override with settings.
        self.update_rollover_alias()
        logger.debug('Rollover alias updated')
        self.update_ilm()
        logger.debug(f'FINAL PLAN: {prettystr(self._plan.toDict())}')

    def update(self, settings: t.Dict) -> None:
        """Update the Plan DotMap"""
        self._plan.update(**settings)

    def update_ilm(self) -> None:
        """Update the ILM portion of the Plan DotMap"""
        setdefault = False
        if 'ilm' not in self._plan:
            logger.debug('key "ilm" is not in plan')
            setdefault = True
        if isinstance(self._plan.ilm, dict):
            _ = DotMap(self._plan.ilm)
            self._plan.ilm = _
        if isinstance(self._plan.ilm, DotMap):
            if 'enabled' not in self._plan.ilm:
                # Override with defaults
                logger.debug(
                    'plan.ilm does not have key "enabled". Overriding with defaults'
                )
                setdefault = True
        elif isinstance(self._plan.ilm, bool):
            if self._plan.ilm:
                logger.warning(
                    '"plan.ilm: True" is incorrect. Use plan.ilm.enabled: True'
                )
            logger.debug('plan.ilm is boolean. Overriding with defaults')
            setdefault = True
        if setdefault:
            logger.debug('Setting defaults for ILM')
            self._plan.ilm = DotMap(TESTPLAN['ilm'])
        if self._plan.ilm.enabled:
            ilm = self._plan.ilm
            if not isinstance(self._plan.ilm.phases, list):
                logger.error('Phases is not a list!')
                self._plan.ilm.phases = TESTPLAN['ilm']['phases']
            for entity in self._plan.index_buildlist:
                if 'searchable' in entity and entity['searchable'] is not None:
                    if not entity['searchable'] in ilm.phases:
                        ilm.phases.append(entity['searchable'])
            logger.debug(f'ILM = {ilm}')
            logger.debug(f'self._plan.ilm = {self._plan.ilm}')
            kwargs = {
                'phases': ilm.phases,
                'forcemerge': ilm.forcemerge,
                'max_num_segments': ilm.max_num_segments,
                'readonly': ilm.readonly,
                'repository': self._plan.repository,
            }
            logger.debug(f'KWARGS = {kwargs}')
            self._plan.ilm.policy = build_ilm_policy(**kwargs)

    def update_rollover_alias(self) -> None:
        """Update the Rollover Alias value"""
        if self._plan.rollover_alias:
            self._plan.rollover_alias = f'{self._plan.prefix}-idx-{self._plan.uniq}'
        else:
            self._plan.rollover_alias = None
