"""Snapshot Entity Manager Class"""

import typing as t
from .entitymgr import EntityMgr
from ..helpers.es_api import do_snap
from ..helpers.utils import getlogger

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

# pylint: disable=missing-docstring


class SnapshotMgr(EntityMgr):
    kind = 'snapshot'
    listname = 'snapshots'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
        autobuild: t.Optional[bool] = False,
    ):
        self.logger = getlogger('es_testbed.SnapshotMgr')
        super().__init__(client=client, plan=plan, autobuild=autobuild)

    def add(self, index: str, tier: str) -> None:
        """Perform a snapshot and add it to the entity_list"""
        msg = f'Creating snapshot of index {index} and mounting in the {tier} tier...'
        self.logger.info(msg)
        do_snap(self.client, self.plan.repository, self.name, index, tier=tier)
        self.appender(self.name)
        self.logger.info('Successfully created snapshot "%s"', self.last)
        self.success = True

    def add_existing(self, name: str) -> None:
        """Add a snapshot that's already been created, e.g. by ILM promotion"""
        self.logger.info('Adding snapshot %s to list...', name)
        self.appender(name)
        self.success = True
