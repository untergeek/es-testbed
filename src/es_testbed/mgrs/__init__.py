"""Make class import nicer"""

from es_testbed.mgrs.component import ComponentMgr
from es_testbed.mgrs.data_stream import DataStreamMgr
from es_testbed.mgrs.ilm import IlmMgr
from es_testbed.mgrs.index import IndexMgr
from es_testbed.mgrs.snapshot import SnapshotMgr
from es_testbed.mgrs.template import TemplateMgr

__all__ = [
    'ComponentMgr',
    'DataStreamMgr',
    'IlmMgr',
    'IndexMgr',
    'SnapshotMgr',
    'TemplateMgr',
]
