"""Utility helper functions"""

import sys
import typing as t
import random
import string
import logging
from datetime import datetime, timezone
from pathlib import Path
from pprint import pformat
from shutil import rmtree
from tempfile import mkdtemp
from git import Repo
from ..defaults import ilm_force_merge, ilm_phase, TIER
from ..exceptions import TestbedMisconfig

logger = logging.getLogger(__name__)


def build_ilm_phase(
    phase: str,
    actions: t.Union[t.Dict, None] = None,
    repo: t.Union[str, None] = None,
    fm: bool = False,
) -> t.Dict:
    """Build a single ILM policy step based on phase"""
    retval = ilm_phase(phase)
    if phase in ['cold', 'frozen']:
        if repo:
            retval[phase]['actions']['searchable_snapshot'] = {
                'snapshot_repository': repo,
                'force_merge_index': fm,
            }
        else:
            msg = (
                f'Unable to build {phase} ILM phase. Value for repository not '
                f'provided'
            )
            raise TestbedMisconfig(msg)
    if actions:
        retval[phase]['actions'].update(actions)
    return retval


def build_ilm_policy(
    phases: list = None,
    forcemerge: bool = False,
    max_num_segments: int = 1,
    readonly: t.Union[str, None] = None,
    repository: t.Union[str, None] = None,
) -> t.Dict:
    """
    Build a full ILM policy based on the provided phases.
    Put forcemerge in the last phase before cold or frozen (whichever comes first)
    """
    if not phases:
        phases = ['hot', 'delete']
    retval = {}
    if ('cold' in phases or 'frozen' in phases) and not repository:
        raise TestbedMisconfig('Cannot build cold or frozen phase without repository')
    for phase in phases:
        actions = None
        if readonly == phase:
            actions = {"readonly": {}}
        phase = build_ilm_phase(phase, repo=repository, fm=forcemerge, actions=actions)
        retval.update(phase)
    if forcemerge:
        retval['hot']['actions'].update(
            ilm_force_merge(max_num_segments=max_num_segments)
        )
    return {'phases': retval}


# def doc_gen(
#     count: int = 10, start_at: int = 0, match: bool = True
# ) -> t.Generator[t.Dict, None, None]:
#     """Create this doc for each count"""
#     keys = ['message', 'nested', 'deep']
#     # Start with an empty map
#     matchmap = {}
#     # Iterate over each key
#     for key in keys:
#         # If match is True
#         if match:
#             # Set matchmap[key] to key
#             matchmap[key] = key
#         else:
#             # Otherwise matchmap[key] will have a random string value
#             matchmap[key] = randomstr()

#     # This is where count and start_at matter
#     for num in range(start_at, start_at + count):
#         yield {
#             '@timestamp': iso8601_now(),
#             'message': f'{matchmap["message"]}{num}',  # message# or randomstr#
#             'number': (
#                 num if match else random.randint(1001, 32767)
#             ),  # value of num or random int
#             'nested': {'key': f'{matchmap["nested"]}{num}'},  # nested#
#             'deep': {'l1': {'l2': {'l3': f'{matchmap["deep"]}{num}'}}},  # deep#
#         }


# def getlogger(name: str) -> logging.getLogger:
#     """Return a named logger"""
#     return logging.getLogger(name)


def get_routing(tier='hot') -> t.Dict:
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
    # Note that the +00:00 is appended now where we affirmatively declare the
    # UTC timezone
    #
    # As a result, we will use this function to prune away the timezone if it is
    # +00:00 and replace it with Z, which is shorter Zulu notation for UTC (which
    # Elasticsearch uses)
    #
    # We are MANUALLY, FORCEFULLY declaring timezone.utc, so it should ALWAYS be
    # +00:00, but could in theory sometime show up as a Z, so we test for that.

    parts = datetime.now(timezone.utc).isoformat().split('+')
    if len(parts) == 1:
        if parts[0][-1] == 'Z':
            return parts[0]  # Our ISO8601 already ends with a Z for Zulu/UTC time
        return f'{parts[0]}Z'  # It doesn't end with a Z so we put one there
    if parts[1] == '00:00':
        return f'{parts[0]}Z'  # It doesn't end with a Z so we put one there
    return f'{parts[0]}+{parts[1]}'  # Fallback publishes the +TZ, whatever that was


def mounted_name(index: str, tier: str):
    """Return a value for renamed_index for mounting a searchable snapshot index"""
    return f'{TIER[tier]["prefix"]}-{index}'


def prettystr(*args, **kwargs) -> str:
    """
    A (nearly) straight up wrapper for pprint.pformat, except that we provide our own
    default values for 'indent' (2) and 'sort_dicts' (False). Primarily for debug
    logging and showing more readable dictionaries.

    'Return the formatted representation of object as a string. indent, width, depth,
    compact, sort_dicts and underscore_numbers are passed to the PrettyPrinter
    constructor as formatting parameters' (from pprint documentation).
    """
    defaults = [
        ('indent', 2),
        ('width', 80),
        ('depth', None),
        ('compact', False),
        ('sort_dicts', False),
        ('underscore_numbers', False),
    ]
    kw = {}
    for tup in defaults:
        key, default = tup
        kw[key] = kwargs[key] if key in kwargs else default

    return f"\n{pformat(*args, **kw)}"  # newline in front so it always starts clean


def process_preset(
    builtin: t.Union[str, None],
    path: t.Union[str, None],
    ref: t.Union[str, None],
    url: t.Union[str, None],
) -> t.Tuple:
    """Process the preset settings
    :param preset: One of `builtin`, `git`, or `path`
    :param builtin: The name of a builtin preset
    :param path: A relative or absolute file path. Used by presets `git` and `path`
    :param ref: A Git ref (e.g. 'main'). Only used by preset `git`
    :param url: A Git repository URL. Only used by preset `git`
    """
    modpath = None
    tmpdir = None
    if builtin:  # Overrides any other options
        modpath = f'es_testbed.presets.{builtin}'
    else:
        trygit = False
        try:
            kw = {'path': path, 'ref': ref, 'url': url}
            raise_on_none(**kw)
            trygit = True  # We have all 3 kwargs necessary for git
        except ValueError as resp:  # Not able to do a git preset
            logger.debug('Unable to import a git-based preset: %s', resp)
        if trygit:  # Trying a git import
            tmpdir = mkdtemp()
            try:
                _ = Repo.clone_from(url, tmpdir, branch=ref, depth=1)
                filepath = Path(tmpdir) / path
            except Exception as err:
                logger.error('Git clone failed: %s', err)
                rmtree(tmpdir)  # Clean up after failed attempt
                raise err
        if path:
            filepath = Path(path)
        if not filepath.resolve().is_dir():
            raise ValueError(f'The provided path "{path}" is not a directory')
        modpath = filepath.resolve().name  # The final dirname
        parent = filepath.parent.resolve()  # Up one level
        # We now make the parent path part of the sys.path.
        sys.path.insert(0, parent)  # This should persist beyond this module
    return modpath, tmpdir


def raise_on_none(**kwargs):
    """Raise if any kwargs have a None value"""
    for key, value in kwargs.items():
        if value is None:
            raise ValueError(f'kwarg "{key}" cannot have a None value')


def randomstr(length: int = 16, lowercase: bool = False) -> str:
    """Generate a random string"""
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str(''.join(random.choices(letters + string.digits, k=length)))


def storage_type(tier: str) -> t.Dict:
    """Return the storage type of a searchable snapshot by tier"""
    return TIER[tier]["storage"]
