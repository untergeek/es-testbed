"""Default values and constants"""

import typing as t

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
        'deep': {
            'properties': {
                'l1': {
                    'properties': {'l2': {'properties': {'l3': {'type': 'keyword'}}}}
                }
            }
        },
    }
}

NAMEMAPPER: t.Dict[str, str] = {
    'index': 'idx',
    'indices': 'idx',  # This is to aid testing and other places where kind is indices
    'data_stream': 'ds',
    'component': 'cmp',
    'ilm': 'ilm',
    'template': 'tmpl',
    'snapshot': 'snp',
}

PAUSE_DEFAULT: str = '1.0'
PAUSE_ENVVAR: str = 'ES_TESTBED_PAUSE'

PLURALMAP: t.Dict[str, str] = {
    'ilm': 'ILM Policies',
    'index': 'indices',
}

TESTPLAN: dict = {
    'type': 'indices',
    'prefix': 'es-testbed',
    'repository': None,
    'rollover_alias': None,
    'ilm': {
        'enabled': False,
        'phases': ['hot', 'delete'],
        'readonly': None,
        'forcemerge': False,
        'max_num_segments': 1,
    },
    'entities': [],
}
"""Default values for the TestPlan settings"""

TIER: dict = {
    'hot': {'pref': 'data_hot,data_content'},
    'warm': {'pref': 'data_warm,data_hot,data_content'},
    'cold': {
        'pref': 'data_cold,data_warm,data_hot,data_content',
        'prefix': 'restored',
        'storage': 'full_copy',
    },
    'frozen': {
        'pref': 'data_frozen',
        'prefix': 'partial',
        'storage': 'shared_cache',
    },
}

TIMEOUT_DEFAULT: str = '30'
TIMEOUT_ENVVAR: str = 'ES_TESTBED_TIMEOUT'

# Define IlmPhase as a typing alias to be reused multiple times
#
# In all currently supported Python versions (3.8 -> 3.12), the syntax:
#
#   IlmPhase = t.Dict[
#
# is supported. In 3.10 and up, you can use the more explicit syntax:
#
#   IlmPhase: t.TypeAlias = t.Dict[
#
# making use of the TypeAlias class.
#
# To support Python versions 3.8 and 3.9 (still), the older syntax is used.
IlmPhase = t.Dict[
    str, t.Union[str, t.Dict[str, str], t.Dict[str, t.Dict[str, t.Dict[str, str]]]]
]


def ilmhot() -> IlmPhase:
    """Return a default hot ILM phase"""
    return {'actions': {'rollover': {'max_primary_shard_size': '1gb', 'max_age': '1d'}}}


def ilmwarm() -> IlmPhase:
    """Return a default warm ILM phase"""
    return {'min_age': '2d', 'actions': {}}


def ilmcold() -> IlmPhase:
    """Return a default cold ILM phase"""
    return {'min_age': '3d', 'actions': {}}


def ilmfrozen() -> IlmPhase:
    """Return a default frozen ILM phase"""
    return {'min_age': '4d', 'actions': {}}


def ilmdelete() -> IlmPhase:
    """Return a default delete ILM phase"""
    return {'min_age': '5d', 'actions': {'delete': {}}}


def ilm_phase(value: str) -> IlmPhase:
    """Return the default phase step based on 'value'"""
    phase_map = {
        'hot': ilmhot(),
        'warm': ilmwarm(),
        'cold': ilmcold(),
        'frozen': ilmfrozen(),
        'delete': ilmdelete(),
    }
    return {value: phase_map[value]}


def ilm_force_merge(max_num_segments: int = 1) -> t.Dict[str, t.Dict[str, int]]:
    """Return an ILM policy force merge action block using max_num_segments"""
    return {'forcemerge': {'max_num_segments': max_num_segments}}
