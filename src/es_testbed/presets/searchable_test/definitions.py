"""Searchable Snapshot Test Built-in Plan"""

import logging
from pathlib import Path
from json import loads
from es_client.helpers.utils import get_yaml
from . import scenarios

logger = logging.getLogger(__name__)


def baseplan() -> dict:
    """Return the base plan object from plan.yml"""
    return get_yaml((modpath() / 'plan.yml'))


def buildlist() -> list:
    """Return the list of index build schemas from buildlist.yml"""
    return get_yaml((modpath() / 'buildlist.yml'))


def get_plan(scenario: str = None) -> dict:
    """Return the plan dict based on scenario"""
    retval = baseplan()
    retval.update(buildlist())
    if not scenario:
        return retval
    retval['uniq'] = f'scenario-{scenario}'
    newvals = getattr(scenarios, scenario)
    ilm = {}
    if 'ilm' in newvals:
        ilm = newvals.pop('ilm')
    if ilm:
        retval['ilm'].update(ilm)
    retval.update(newvals)
    return retval


def mappings() -> dict:
    """Return the index mappings from mappings.json"""
    return loads((modpath() / 'mappings.json').read_text(encoding='UTF-8'))


def modpath() -> Path:
    """Return the local file path"""
    return Path(__file__).parent.resolve()


def settings() -> dict:
    """Return the index settings from settings.json"""
    return loads((modpath() / 'settings.json').read_text(encoding='UTF-8'))
