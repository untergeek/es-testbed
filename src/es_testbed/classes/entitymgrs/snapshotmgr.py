"""Snapshot Entity Manager Class"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .entitymgr import EntityMgr
from ..testplan import TestPlan

# pylint: disable=missing-docstring

class SnapshotMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = False,
        ):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.kind = 'snapshot'
        self.logger = getlogger('es_testbed.SnapshotMgr')
        # We do not autobuild in this class

    def add(self, index: str, tier: str) -> None:
        msg = f'Creating snapshot of index {index} and mounting in the {tier} tier...'
        self.logger.info(msg)
        es_api.do_snap(
            self.client,
            self.plan.ilm.repository,
            self.name,
            index,
            tier=tier
        )
        self.entity_list.append(self.name)
        self.logger.info('Successfully created snapshot "%s"', self.last)
        self.success = True

    def add_existing(self, name: str) -> None:
        """Add a snapshot that's already been created, e.g. by ILM promotion"""
        self.logger.info('Adding snapshot %s to list...', name)
        self.entity_list.append(name)
        self.success = True

    def setup(self):
        pass

    def teardown(self):
        if not self.success:
            self.logger.info('No snapshots to clean up.')
            return
        self.logger.info('Cleaning up any existing snapshots...')
        for snapshot in self.entity_list:
            es_api.delete(self.client, 'snapshot', snapshot, repository=self.plan.ilm.repository)
        self.logger.info('Cleanup of snapshots completed.')