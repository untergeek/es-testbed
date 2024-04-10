"""Default values and constants"""

# pylint: disable=E1120

EPILOG: str = 'Learn more at https://github.com/untergeek/es-testbed'

HELP_OPTIONS: dict = {'help_option_names': ['-h', '--help']}

MAPPING: dict = {
    'properties': {
        'message': {'type': 'keyword'},
        'number': {'type': 'long'},
        'nested': {'properties': {'key': {'type': 'keyword'}}},
        'deep': {'properties': {'l1': {'properties': {'l2': {
            'properties': {'l3': {'type': 'keyword'}}}}}}
        }
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
        'pfx': 'restored',
        'sto': 'full_copy',
    },
    'frozen': {
        'pref': 'data_frozen',
        'pfx': 'partial',
        'sto': 'shared_cache',
    }
}

def ilm_phase(tier):
    """Return the default phase step based on 'tier'"""
    phase_map = {
        'hot': {
            'actions': {
                'rollover': {
                    'max_primary_shard_size': '1gb',
                    'max_age': '10m'
                }
            }
        },
        'warm': {'min_age': '30m', 'actions': {}},
        'cold': {"min_age": "1h", "actions": {}},
        'frozen': {"min_age": "2h", "actions": {}},
        'delete': {"min_age": "3h", "actions": {"delete": {}}}   
    }
    return {tier: phase_map[tier]}

def ilm_force_merge(max_num_segments=1):
    """Return an ILM policy force merge action block using max_num_segments"""
    return {'forcemerge': {'max_num_segments': max_num_segments}}
