"""Datastream Entity Manager Class"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.defaults import NAMEMAPPER
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .indexmgr import IndexMgr
from .snapshotmgr import SnapshotMgr
from ..entities import Datastream, Index
from ..testplan import TestPlan

# pylint: disable=missing-docstring,too-many-arguments

class DatastreamMgr(IndexMgr):
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = True,
            snapmgr: SnapshotMgr = None,
            policy_name: str = None,
        ):
        super().__init__(
            client=client, plan=plan, autobuild=autobuild, snapmgr=snapmgr, policy_name=policy_name)
        self.kind = 'datastream'
        self.logger = getlogger('es_testbed.DatastreamMgr')
        self.ds = None
        self.index_trackers = []

    @property
    def suffix(self): # Remapping this to send no suffix for datastreams
        return ''

    @property
    def tracked_index_list(self) -> t.Sequence[str]:
        return [x.name for x in self.index_trackers]

    def add(self, value):
        es_api.create_datastream(self.client, value)
        self.track_datastream()

    # Some weird inheritance here was not picking up self.kind properly, so I override here
    def ident(self, dkey='datastream'):
        return NAMEMAPPER[dkey]

    def searchable(self):
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.entities):
            self.index_trackers[idx].mount_ss(scheme)
        self.logger.info('Completed backing index promotion to searchable snapshots.')
        self.logger.info('Datastream backing indices: %s', self.ds.backing_indices)

    def setup(self) -> bool:
        self.index_trackers = [] # Inheritance oddity requires redeclaration here
        for scheme in self.plan.entities:
            if not self.entity_list:
                self.add(self.name)
            else:
                self.ds.rollover()
            self.filler(scheme)
        self.logger.debug('Created datastream: %s', self.ds.name)
        self.logger.debug('Created datastream backing indices: %s', self.ds.backing_indices)
        for index in self.ds.backing_indices:
            self.track_index(index)
        self.ds.verify(self.tracked_index_list)
        self.searchable()
        self.ds.verify(self.tracked_index_list)
        self.logger.info('Successfully completed datastream buildout.')
        self.success = True

    def teardown(self):
        if not self.success:
            msg = (
                'Setup did not complete successfully. '
                'Manual cleanup of datastreams may be necessary.'
            )
            self.logger.warning(msg)
            return
        self.logger.info('Cleaning up datastream %s', self.last)
        for dstream in self.entity_list: # There should only be one
            es_api.delete(self.client, 'datastream', dstream)
        self.logger.info('Cleanup of datastream %s complete.', self.last)

    def track_datastream(self) -> None:
        entity = Datastream(client=self.client, name=self.name)
        self.ds = entity
        self.entity_list.append(self.name)

    def track_index(self, name: str) -> None:
        entity = Index(
            client=self.client,
            name=name,
            snapmgr=self.snapmgr,
            policy_name=self.policy_name
        )
        self.index_trackers.append(entity)
