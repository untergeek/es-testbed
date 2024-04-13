"""Utility helper functions"""
import random
import string
from es_testbed.defaults import ilm_force_merge, ilm_phase, MAPPING, TIER
from es_testbed.exceptions import TestbedMisconfig

def randomstr(length: int=16, lowercase: bool=False):
    """Generate a random string"""
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str(''.join(random.choices(letters + string.digits, k=length)))

def doc_gen(count=10, start_at=0, match=True):
    """Create this doc for each count"""
    keys = ['message', 'nested', 'deep']
    # Start with an empty map
    matchmap = {}
    # Iterate over each key
    for key in keys:
        # If match is True
        if match:
            # Set matchmap[key] to key
            matchmap[key] = key
        else:
            # Otherwise matchmap[key] will have a random string value
            matchmap[key] = randomstr()

    # This is where count and start_at matter
    for num in range(start_at, start_at + count):
        yield {
            'message': f'{matchmap["message"]}{num}', # message# or randomstr#
            'number': num if match else random.randint(1001, 32767), # value of num or random int
            'nested': {
                'key': f'{matchmap["nested"]}{num}' # nested#
            },
            'deep': {
                'l1': {
                    'l2': {
                        'l3': f'{matchmap["deep"]}{num}' # deep#
                    }
                }
            }
        }

def build_ilm_phase(tier, actions=None, repository=None):
    """Build a single ILM policy step based on tier"""
    phase = ilm_phase(tier)
    if tier in ['cold', 'frozen']:
        if repository:
            phase[tier]['actions']['searchable_snapshot'] = {'snapshot_repository': repository}
        else:
            msg = f'Unable to build ILM phase for {tier} tier. Value for repository not provided'
            raise TestbedMisconfig(msg)
    if actions:
        phase[tier]['actions'].update(actions)
    return phase

def build_ilm_policy(tiers: list=None, fmerge: bool=False, mns: int=1, repo: str=None):
    """
    Build a full ILM policy based on the provided tiers.
    Put forcemerge in the last tier before cold or frozen (whichever comes first)
    """
    if not tiers:
        tiers = ['hot', 'delete']
    phases = {}
    if ('cold' in tiers or 'frozen' in tiers) and not repo:
        raise TestbedMisconfig('Cannot build cold or frozen phase without repository')
    for tier in tiers:
        phases.update(build_ilm_phase(tier, repository=repo))
    if fmerge:
        phases['hot']['actions'].update(ilm_force_merge(max_num_segments=mns))
    return {'phases': phases}

def ds_action_generator(datastream: str, index: str, action: str=None):
    """Generate a single add or remove backing index action for a datastream"""
    if not action or action not in ['add', 'remove']:
        raise TestbedMisconfig('action must be "add" or "remove"')
    return {
        f'{action}_backing_index': {
            'data_stream': datastream,
            'index': index
        }
    }

def get_routing(tier='hot'):
    """Return the routing allocation tier preference"""
    try:
        pref = TIER[tier]['pref']
    except KeyError:
        # Fallback value
        pref = 'data_content'
    return {'index.routing.allocation.include._tier_preference': pref}

def mapping_component():
    """Return a mappings component template"""
    return {'mappings': MAPPING}

def mounted_name(index, tier):
    """Return a value for renamed_index for mounting a searchable snapshot index"""
    return f'{TIER[tier]["prefix"]}-{index}'

def setting_component(ilm_policy: str=None, rollover_alias: str=None):
    """Return a settings component template"""
    val = {'settings':{}}
    if ilm_policy:
        val['settings']['index.lifecycle.name'] = ilm_policy
    if rollover_alias:
        val['settings']['index.lifecycle.rollover_alias'] = rollover_alias
    return val

def storage_type(tier):
    """Return the storage type of a searchable snapshot by tier"""
    return TIER[tier]["storage"]
