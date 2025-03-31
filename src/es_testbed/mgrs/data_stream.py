"""data_stream Entity Manager Class"""

import typing as t
import logging
from importlib import import_module
import tiered_debug as debug
from es_testbed.entities import DataStream, Index
from es_testbed.helpers.es_api import create_data_stream, fill_index
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
        debug.lv3('Initializing DataStreamMgr object...')
        super().__init__(client=client, plan=plan, snapmgr=snapmgr)
        debug.lv3('DataStreamMgr object initialized')

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
        debug.lv2('Starting method...')
        try:
            debug.lv4(f'TRY: Creating data_stream {value}')
            create_data_stream(self.client, value)
        except Exception as err:
            logger.critical(f'Error creating data_stream: {prettystr(err)}')
            raise err
        self.track_data_stream()
        debug.lv3('Exiting method')

    def searchable(self):
        """If the indices were marked as searchable snapshots, we do that now"""
        debug.lv2('Starting method...')
        for idx, scheme in enumerate(self.plan.index_buildlist):
            self.index_trackers[idx].mount_ss(scheme)
        logger.info('Completed backing index promotion to searchable snapshots.')
        debug.lv5(f'data_stream backing indices: {prettystr(self.ds.backing_indices)}')
        debug.lv3('Exiting method')

    def setup(self) -> None:
        """Setup the entity manager"""
        debug.lv2('Starting method...')
        self.index_trackers = []  # Inheritance oddity requires redeclaration here
        mod = import_module(f'{self.plan.modpath}.functions')
        func = getattr(mod, 'doc_generator')
        for scheme in self.plan.index_buildlist:
            if not self.entity_list:
                self.add(self.name)
            else:
                self.ds.rollover()
            fill_index(
                self.client,
                name=self.name,
                doc_generator=func,
                options=scheme['options'],
            )
        debug.lv2(f'Created data_stream: {self.ds.name}')
        debug.lv3(
            f'Created data_stream backing indices: {prettystr(self.ds.backing_indices)}'
        )
        for index in self.ds.backing_indices:
            self.track_index(index)
        self.ds.verify(self.indexlist)
        self.searchable()
        self.ds.verify(self.indexlist)
        logger.info('Successfully completed data_stream buildout.')
        debug.lv3('Exiting method')

    def track_data_stream(self) -> None:
        """Add a DataStream entity and append it to entity_list"""
        debug.lv2('Starting method...')
        debug.lv3(f'Tracking data_stream: {self.name}')
        self.ds = DataStream(client=self.client, name=self.name)
        self.appender(self.name)
        debug.lv3('Exiting method')

    def track_index(self, name: str) -> None:
        """Add an Index entity and append it to index_trackers"""
        debug.lv2('Starting method...')
        debug.lv3(f'Tracking index: "{name}"')
        entity = Index(
            client=self.client,
            name=name,
            snapmgr=self.snapmgr,
            policy_name=self.policy_name,
        )
        entity.track_ilm(name)
        self.index_trackers.append(entity)
        debug.lv3('Exiting method')
