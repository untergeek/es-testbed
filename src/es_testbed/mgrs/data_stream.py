"""data_stream Entity Manager Class"""

import typing as t
import logging
from es_testbed.entities import DataStream, Index
from es_testbed.helpers.es_api import create_data_stream
from es_testbed.helpers.utils import prettystr
from es_testbed.mgrs.index import IndexMgr
from es_testbed.mgrs.snapshot import SnapshotMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class DataStreamMgr(IndexMgr):
    """data_stream Entity Manager Class"""

    kind = 'data_stream'
    listname = 'data_stream'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
        snapmgr: t.Union[SnapshotMgr, None] = None,
    ):
        self.ds = None
        self.index_trackers = []
        super().__init__(client=client, plan=plan, snapmgr=snapmgr)

    @property
    def suffix(self):
        """Return nothing, as there is no index count suffix to a data_stream name"""
        return ''

    @property
    def indexlist(self) -> t.Sequence[str]:
        """Get the current list of indices in the data_stream"""
        return [x.name for x in self.index_trackers]

    def add(self, value):
        """Create a data stream and track it"""
        create_data_stream(self.client, value)
        self.track_data_stream()

    def searchable(self):
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.entities):
            self.index_trackers[idx].mount_ss(scheme)
        logger.info('Completed backing index promotion to searchable snapshots.')
        logger.info(
            'data_stream backing indices: %s', prettystr(self.ds.backing_indices)
        )

    def setup(self) -> None:
        """Setup the entity manager"""
        self.index_trackers = []  # Inheritance oddity requires redeclaration here
        for scheme in self.plan.entities:
            if not self.entity_list:
                self.add(self.name)
            else:
                self.ds.rollover()
            self.filler(scheme)
        logger.debug('Created data_stream: %s', self.ds.name)
        logger.debug(
            'Created data_stream backing indices: %s',
            prettystr(self.ds.backing_indices),
        )
        for index in self.ds.backing_indices:
            self.track_index(index)
        self.ds.verify(self.indexlist)
        self.searchable()
        self.ds.verify(self.indexlist)
        logger.info('Successfully completed data_stream buildout.')

    def track_data_stream(self) -> None:
        """Add a DataStream entity and append it to entity_list"""
        logger.debug('Tracking data_stream: %s', self.name)
        self.ds = DataStream(client=self.client, name=self.name)
        self.appender(self.name)

    def track_index(self, name: str) -> None:
        """Add an Index entity and append it to index_trackers"""
        logger.debug('Tracking index: "%s"', name)
        entity = Index(
            client=self.client,
            name=name,
            snapmgr=self.snapmgr,
            policy_name=self.policy_name,
        )
        entity.track_ilm(name)
        self.index_trackers.append(entity)
