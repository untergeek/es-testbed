"""Snapshot Entity Manager Class"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .entitymgr import EntityMgr

# pylint: disable=missing-docstring


class SnapshotMgr(EntityMgr):
    kind = 'snapshot'
    listname = 'snapshots'

    def __init__(self, client: Elasticsearch = None, plan: DotMap = None, autobuild: t.Optional[bool] = False):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.SnapshotMgr')

    def add(self, index: str, tier: str) -> None:
        msg = f'Creating snapshot of index {index} and mounting in the {tier} tier...'
        self.logger.info(msg)
        es_api.do_snap(self.client, self.plan.repository, self.name, index, tier=tier)
        self.appender(self.name)
        self.logger.info('Successfully created snapshot "%s"', self.last)
        self.success = True

    def add_existing(self, name: str) -> None:
        """Add a snapshot that's already been created, e.g. by ILM promotion"""
        self.logger.info('Adding snapshot %s to list...', name)
        self.appender(name)
        self.success = True

    def setup(self):
        pass

    def teardown(self):  # We override the parent method here to speed things up.
        if self.entity_list:
            if not self.success:
                msg = f'Setup did not complete successfully. ' f'Manual cleanup of {self.kind}s may be necessary.'
                self.logger.warning(msg)
            self.logger.info('Cleaning up any existing snapshots...')
            es_api.delete(self.client, self.kind, ','.join(self.entity_list), repository=self.plan.repository)
            self.logger.info('Cleanup of snapshots completed.')
            self.entity_list = []
            self.failsafe = []
