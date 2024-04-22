"""Default values and constants"""
import typing as t
# pylint: disable=E1120

EPILOG: str = 'Learn more at https://github.com/untergeek/es-testbed'

HELP_OPTIONS: dict = {'help_option_names': ['-h', '--help']}

ARGSCLASSES: list = ['IlmBuilder', 'IlmExplain', 'TestPlan']

COLD_PREFIX: str = 'restored-'
FROZEN_PREFIX: str = 'partial-'

SS_PREFIX: t.Dict[str, str] = {'cold': COLD_PREFIX, 'frozen': FROZEN_PREFIX}

MAPPING: dict = {
    'properties': {
        '@timestamp': {'type': 'date'},
        'message': {'type': 'keyword'},
        'number': {'type': 'long'},
        'nested': {'properties': {'key': {'type': 'keyword'}}},
        'deep': {'properties': {'l1': {'properties': {'l2': {
            'properties': {'l3': {'type': 'keyword'}}}}}}
        }
    }
}

NAMEMAPPER: t.Dict[str, str] = {
    'index': 'idx',
    'data_stream': 'ds',
    'component': 'cmp',
    'ilm': 'ilm',
    'template': 'tmpl',
    'snapshot': 'snp',
}

PAUSE_DEFAULT: str = '0.25'
PAUSE_ENVVAR: str = 'ES_TESTBED_PAUSE'

PLURALMAP: t.Dict[str, str] = {
    'ilm': 'ILM Policie',
    'index': 'indice',
}

TESTPLAN: dict = {
    'type': 'indices',
    'prefix': 'es-testbed',
    'rollover_alias': False,
    'ilm': {
        'tiers': ['hot', 'delete'],
        'forcemerge': False,
        'max_num_segments': 1,
        'repository': None,
    },
    'defaults': {
        'entity_count': 3,
        'docs': 10,
        'match': True,
        'searchable': None,
    }
}

TIER: dict = {
    'hot': {
        'pref': 'data_hot,data_content'
    },
    'warm': {
        'pref': 'data_warm,data_hot,data_content'
    },
    'cold': {
        'pref': 'data_cold,data_warm,data_hot,data_content',
        'prefix': 'restored',
        'storage': 'full_copy',
    },
    'frozen': {
        'pref': 'data_frozen',
        'prefix': 'partial',
        'storage': 'shared_cache',
    }
}

def ilm_phase(tier):
    """Return the default phase step based on 'tier'"""
    phase_map = {
        'hot': {
            'actions': {
                'rollover': {
                    'max_primary_shard_size': '1gb',
                    'max_age': '1d'
                }
            }
        },
        'warm': {'min_age': '2d', 'actions': {}},
        'cold': {"min_age": "3d", "actions": {}},
        'frozen': {"min_age": "4d", "actions": {}},
        'delete': {"min_age": "5d", "actions": {"delete": {}}}   
    }
    return {tier: phase_map[tier]}

def ilm_force_merge(max_num_segments=1):
    """Return an ILM policy force merge action block using max_num_segments"""
    return {'forcemerge': {'max_num_segments': max_num_segments}}
