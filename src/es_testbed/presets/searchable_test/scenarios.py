"""
Searchable Snapshot Test Scenarios

We use deepcopy to ensure that other scenarios changes don't propagate. We can't use
constants as the values need to be changed, so deepcopy has us covered.

The repository should be configurable at runtime, so we make it use the TEST_ES_REPO
env var.
"""

from os import environ
from copy import deepcopy

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

bl = [
    {
        'options': {
            'count': 10,
            'start_at': 0,
            'match': True,
        },
        'target_tier': 'frozen',
    },
    {
        'options': {
            'count': 10,
            'start_at': 10,
            'match': True,
        },
        'target_tier': 'frozen',
    },
    {
        'options': {
            'count': 10,
            'start_at': 20,
            'match': True,
        },
        'target_tier': 'hot',
    },
]

# #####################
# ### Hot Scenarios ###
# #####################

hot = {}  # The default
hot_rollover = {'rollover_alias': True}
hot_ds = {'type': 'data_stream'}

# ######################
# ### Cold Scenarios ###
# ######################

# Define the basic cold scenario
cold = {'repository': REPOSITORY, 'index_buildlist': deepcopy(bl)}
cold['index_buildlist'][0]['target_tier'] = 'cold'
cold['index_buildlist'][1]['target_tier'] = 'cold'
cold['index_buildlist'][2]['target_tier'] = 'cold'

# Define the rollover_alias cold scenario
cold_rollover = deepcopy(cold)
cold_rollover['index_buildlist'][2]['target_tier'] = 'hot'
cold_rollover['rollover_alias'] = True

# Define the cold ILM scenario
cold_ilm = {
    'repository': REPOSITORY,
    'rollover_alias': True,
    'ilm': {'enabled': True, 'phases': ['hot', 'cold', 'delete']},
}

# Define the cold data_stream scenario
cold_ds = deepcopy(cold_ilm)
cold_ds['type'] = 'data_stream'

# ########################
# ### Frozen Scenarios ###
# ########################

# Define the basic frozen scenario
frozen = {'repository': REPOSITORY, 'index_buildlist': deepcopy(bl)}
frozen['index_buildlist'][0]['target_tier'] = 'frozen'
frozen['index_buildlist'][1]['target_tier'] = 'frozen'
frozen['index_buildlist'][2]['target_tier'] = 'frozen'

# Define the rollover_alias frozen scenario
frozen_rollover = deepcopy(frozen)
frozen_rollover['index_buildlist'][2]['target_tier'] = 'hot'
frozen_rollover['rollover_alias'] = True

# Define the frozen ILM scenario
frozen_ilm = {
    'repository': REPOSITORY,
    'rollover_alias': True,
    'ilm': {'enabled': True, 'phases': ['hot', 'frozen', 'delete']},
}

# Define the frozen data_stream scenario
frozen_ds = deepcopy(frozen_ilm)
frozen_ds['type'] = 'data_stream'
