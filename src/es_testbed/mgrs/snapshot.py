"""Snapshot Entity Manager Class"""

import typing as t
import logging
import tiered_debug as debug
from es_testbed.helpers.es_api import do_snap
from es_testbed.mgrs.entity import EntityMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class SnapshotMgr(EntityMgr):
    """Snapshot Entity Manager Class"""

    kind = 'snapshot'
    listname = 'snapshots'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
    ):
        debug.lv2('Initializing SnapshotMgr object...')
        super().__init__(client=client, plan=plan)
        debug.lv3('SnapshotMgr object initialized')

    def add(self, index: str, tier: str) -> None:
        """Perform a snapshot and add it to the entity_list"""
        debug.lv2('Starting method...')
        msg = f'Creating snapshot of index {index} and mounting in the {tier} tier...'
        debug.lv3(msg)
        do_snap(self.client, self.plan.repository, self.name, index, tier=tier)
        self.appender(self.name)
        debug.lv3(f'Successfully created snapshot "{self.last}"')
        debug.lv3('Exiting method')

    def add_existing(self, name: str) -> None:
        """Add a snapshot that's already been created, e.g. by ILM promotion"""
        debug.lv2('Starting method...')
        debug.lv3(f'Adding snapshot {name} to list...')
        self.appender(name)
        debug.lv3('Exiting method')
