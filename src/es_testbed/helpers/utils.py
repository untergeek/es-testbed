"""Utility helper functions"""

import typing as t
import random
import string
import logging
from copy import deepcopy
from datetime import datetime, timezone
from es_testbed.defaults import ilm_force_merge, ilm_phase, MAPPING, TIER
from es_testbed.exceptions import TestbedMisconfig


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


def build_ilm_policy(tiers: list = None, forcemerge: bool = False, max_num_segments: int = 1, repository: str = None):
    """
    Build a full ILM policy based on the provided tiers.
    Put forcemerge in the last tier before cold or frozen (whichever comes first)
    """
    if not tiers:
        tiers = ['hot', 'delete']
    phases = {}
    if ('cold' in tiers or 'frozen' in tiers) and not repository:
        raise TestbedMisconfig('Cannot build cold or frozen phase without repository')
    for tier in tiers:
        phases.update(build_ilm_phase(tier, repository=repository))
    if forcemerge:
        phases['hot']['actions'].update(ilm_force_merge(max_num_segments=max_num_segments))
    return {'phases': phases}


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
            '@timestamp': iso8601_now(),
            'message': f'{matchmap["message"]}{num}',  # message# or randomstr#
            'number': num if match else random.randint(1001, 32767),  # value of num or random int
            'nested': {'key': f'{matchmap["nested"]}{num}'},  # nested#
            'deep': {'l1': {'l2': {'l3': f'{matchmap["deep"]}{num}'}}},  # deep#
        }


def ds_action_generator(
    data_stream: str, index: str, action: t.Union[t.Literal['add'], t.Literal['remove'], None] = None
) -> t.Dict:
    """Generate a single add or remove backing index action for a data_stream"""
    if not action or action not in ['add', 'remove']:
        raise TestbedMisconfig('action must be "add" or "remove"')
    return {f'{action}_backing_index': {'data_stream': data_stream, 'index': index}}


def getlogger(name: str) -> logging.getLogger:
    """Return a named logger"""
    return logging.getLogger(name)


def get_routing(tier='hot'):
    """Return the routing allocation tier preference"""
    try:
        pref = TIER[tier]['pref']
    except KeyError:
        # Fallback value
        pref = 'data_content'
    return {'index.routing.allocation.include._tier_preference': pref}


def iso8601_now() -> str:
    """
    :returns: An ISO8601 timestamp based on now
    :rtype: str
    """
    # Because Python 3.12 now requires non-naive timezone declarations, we must change.
    #
    # ## Example:
    # ## The new way:
    # ##     datetime.now(timezone.utc).isoformat()
    # ##     Result: 2024-04-16T16:00:00+00:00
    # ## End Example
    #
    # Note that the +00:00 is appended now where we affirmatively declare the UTC timezone
    #
    # As a result, we will use this function to prune away the timezone if it is +00:00 and replace
    # it with Z, which is shorter Zulu notation for UTC (which Elasticsearch uses)
    #
    # We are MANUALLY, FORCEFULLY declaring timezone.utc, so it should ALWAYS be +00:00, but could
    # in theory sometime show up as a Z, so we test for that.

    parts = datetime.now(timezone.utc).isoformat().split('+')
    if len(parts) == 1:
        if parts[0][-1] == 'Z':
            return parts[0]  # Our ISO8601 already ends with a Z for Zulu/UTC time
        return f'{parts[0]}Z'  # It doesn't end with a Z so we put one there
    if parts[1] == '00:00':
        return f'{parts[0]}Z'  # It doesn't end with a Z so we put one there
    return f'{parts[0]}+{parts[1]}'  # Fallback publishes the +TZ, whatever that was


def mapping_component():
    """Return a mappings component template"""
    return {'mappings': MAPPING}


def mounted_name(index, tier):
    """Return a value for renamed_index for mounting a searchable snapshot index"""
    return f'{TIER[tier]["prefix"]}-{index}'


def posmatch(original: t.Sequence[str], compare: t.Sequence[str]) -> t.Sequence[int]:
    """
    Compare the values in compare with original. Return a list of index positions from
    compare of any values that match.
    """
    logger = getlogger(__name__)
    positions = []
    for orig in original:
        for idx, comp in enumerate(compare):
            if comp == orig:
                logger.debug('Value %s found in both original and compare', comp)
                positions.append(idx)
    return positions


def randomstr(length: int = 16, lowercase: bool = False):
    """Generate a random string"""
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str(''.join(random.choices(letters + string.digits, k=length)))


def remove_by_index(original: t.Sequence[str], compare: t.Sequence[str]) -> t.Sequence[str]:
    """By list index position, remove entries from compare which also exist in original"""
    logger = getlogger(__name__)
    positions = posmatch(original, compare)
    if not positions:
        logger.debug('No matching values to remove')
        return compare
    # If you del list[0] first, you get a new list[0]
    # We need to delete from the highest index value to the lowest
    positions.sort()  # Sort first to ensure lowest to highest order
    for idx in reversed(positions):  # Reverse the list and iterate
        del compare[idx]  # Delete the value at position idx
    return compare


def setting_component(
    ilm_policy: t.Union[str, None] = None, rollover_alias: t.Union[str, None] = None
) -> t.Dict[str, t.Any]:
    """Return a settings component template"""
    val = {'settings': {'index.number_of_replicas': 0}}
    if ilm_policy:
        val['settings']['index.lifecycle.name'] = ilm_policy
    if rollover_alias:
        val['settings']['index.lifecycle.rollover_alias'] = rollover_alias
    return val


def storage_type(tier: str) -> t.Dict:
    """Return the storage type of a searchable snapshot by tier"""
    return TIER[tier]["storage"]


def uniq_values(original: t.Sequence[str], compare: t.Sequence[str]) -> t.Sequence[str]:
    """Return any values unique to list 'compare'"""
    logger = getlogger(__name__)
    # Use deepcopy of compare so we don't change the original
    uniq = remove_by_index(original, deepcopy(compare))
    if uniq:
        logger.debug('Values found only in compare: %s', uniq)
    return uniq
