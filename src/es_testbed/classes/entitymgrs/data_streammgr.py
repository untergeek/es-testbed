"""data_stream Entity Manager Class"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .indexmgr import IndexMgr
from .snapshotmgr import SnapshotMgr
from ..entities import DataStream, Index

# pylint: disable=missing-docstring,too-many-arguments,broad-exception-caught


class DataStreamMgr(IndexMgr):
    kind = 'data_stream'
    listname = 'data_stream'

    def __init__(
        self,
        client: Elasticsearch = None,
        plan: DotMap = None,
        autobuild: t.Optional[bool] = True,
        snapmgr: SnapshotMgr = None,
    ):
        self.ds = None
        self.index_trackers = []
        super().__init__(client=client, plan=plan, autobuild=autobuild, snapmgr=snapmgr)
        self.logger = getlogger('es_testbed.DataStreamMgr')

    @property
    def suffix(self):  # Remapping this to send no suffix for data_streams
        return ''

    @property
    def indexlist(self) -> t.Sequence[str]:
        return [x.name for x in self.index_trackers]

    def add(self, value):
        es_api.create_data_stream(self.client, value)
        self.track_data_stream()

    def searchable(self):
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.entities):
            self.index_trackers[idx].mount_ss(scheme)
        self.logger.info('Completed backing index promotion to searchable snapshots.')
        self.logger.info('data_stream backing indices: %s', self.ds.backing_indices)

    def setup(self) -> bool:
        self.index_trackers = []  # Inheritance oddity requires redeclaration here
        for scheme in self.plan.entities:
            if not self.entity_list:
                self.add(self.name)
            else:
                self.ds.rollover()
            self.filler(scheme)
        self.logger.debug('Created data_stream: %s', self.ds.name)
        self.logger.debug('Created data_stream backing indices: %s', self.ds.backing_indices)
        for index in self.ds.backing_indices:
            self.track_index(index)
        self.ds.verify(self.indexlist)
        self.searchable()
        self.ds.verify(self.indexlist)
        self.logger.info('Successfully completed data_stream buildout.')
        self.success = True

    def track_data_stream(self) -> None:
        self.logger.debug('Tracking data_stream: %s', self.name)
        self.ds = DataStream(client=self.client, name=self.name)
        self.appender(self.name)

    def track_index(self, name: str) -> None:
        self.logger.debug('Tracking index: %s', name)
        entity = Index(client=self.client, name=name, snapmgr=self.snapmgr, policy_name=self.policy_name)
        self.index_trackers.append(entity)
