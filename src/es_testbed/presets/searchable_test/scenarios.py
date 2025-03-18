"""
Searchable Snapshot Test Scenarios

The repository should be configurable at runtime, so we make it use the TEST_ES_REPO
env var.
"""

from os import environ
import typing as t
import logging

logger = logging.getLogger(__name__)

REPOSITORY = environ.get('TEST_ES_REPO', 'found-snapshots')

# default values = {
#     'type': 'indices',
#     'rollover_alias': False,
#     'repository': REPOSITORY,
#     'ilm': {
#         'enabled': False,
#         'phases': ['hot', 'delete'],
#         'readonly': None,
#         'max_num_segments': 1,
#         'forcemerge': False,
#     },
# }


class Scenarios:
    """Scenarios Class"""

    def __init__(self):
        """Initialize the scenarios for this preset"""
        _ = 'init'  # Add to avoid empty function body

    # #####################
    # ### Hot Scenarios ###
    # #####################

    @property
    def hot(self):
        """Basic hot scenario"""
        return self.scenario_builder('hot', 'basic')

    @property
    def hot_rollover(self):
        """Hot rollover index scenario"""
        return self.scenario_builder('hot', 'rollover')

    @property
    def hot_ds(self):
        """Hot data_stream scenario"""
        return self.scenario_builder('hot', 'data_stream')

    # ######################
    # ### Cold Scenarios ###
    # ######################

    @property
    def cold(self):
        """Basic cold scenario"""
        return self.scenario_builder('cold', 'basic')

    @property
    def cold_rollover(self):
        """Cold rollover index scenario"""
        return self.scenario_builder('cold', 'rollover')

    @property
    def cold_ilm(self):
        """Cold ilm index scenario"""
        return self.scenario_builder('cold', 'ilm')

    @property
    def cold_ds(self):
        """Cold data_stream scenario"""
        return self.scenario_builder('cold', 'data_stream')

    # ########################
    # ### Frozen Scenarios ###
    # ########################

    @property
    def frozen(self):
        """Basic frozen scenario"""
        return self.scenario_builder('frozen', 'basic')

    @property
    def frozen_rollover(self):
        """Frozen rollover index scenario"""
        return self.scenario_builder('frozen', 'rollover')

    @property
    def frozen_ilm(self):
        """Frozen ilm index scenario"""
        return self.scenario_builder('frozen', 'ilm')

    @property
    def frozen_ds(self):
        """Frozen data_stream scenario"""
        return self.scenario_builder('frozen', 'data_stream')

    def build_list(self) -> t.Sequence[t.Dict]:
        """The plan build list for these scenarios"""
        return [
            {
                'options': {
                    'count': 10,
                    'start_at': i * 10,
                    'match': True,
                },
                'target_tier': 'frozen' if i < 2 else 'hot',
            }
            for i in range(3)
        ]

    def define_ilm(
        self,
        phase: t.Literal['hot', 'warm', 'cold', 'frozen'],
        subtype: t.Literal['basic', 'rollover', 'ilm', 'data_stream'],
    ) -> t.Union[t.Dict, bool]:
        """Return ILM based on subtype"""
        retval = {'enabled': False, 'phases': ['hot', 'delete']}
        if phase in ['cold', 'frozen']:
            retval['phases'].append(phase)
        if subtype in ['ilm', 'data_stream']:
            retval['enabled'] = True
        return retval

    def scenario_builder(
        self,
        phase: t.Literal['hot', 'warm', 'cold', 'frozen'] = 'hot',
        subtype: t.Literal['basic', 'rollover', 'ilm', 'data_stream'] = 'basic',
    ) -> t.Dict:
        """Build a scenario"""
        retval = {
            'repository': REPOSITORY,
            'index_buildlist': self.build_list(),
            'ilm': {'enabled': False, 'phases': ['hot', 'delete']},
        }
        for idx in range(len(retval['index_buildlist'])):
            # By default, make everything have the phase as the target_tier
            retval['index_buildlist'][idx]['target_tier'] = phase
        if subtype != 'basic':
            # The last entry should always be hot in rollover, ilm, and data_stream
            retval['index_buildlist'][-1]['target_tier'] = 'hot'
        if subtype in ['rollover', 'ilm']:
            retval['rollover_alias'] = True
        retval['ilm'] = self.define_ilm(phase, subtype)
        retval['type'] = 'data_stream' if subtype == 'data_stream' else 'indices'
        return retval
