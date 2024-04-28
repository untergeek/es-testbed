"""Make class import nicer"""

from .componentmgr import ComponentMgr
from .data_streammgr import DataStreamMgr
from .ilmmgr import IlmMgr
from .indexmgr import IndexMgr
from .snapshotmgr import SnapshotMgr
from .templatemgr import TemplateMgr

__all__ = [
    'ComponentMgr',
    'DataStreamMgr',
    'IlmMgr',
    'IndexMgr',
    'SnapshotMgr',
    'TemplateMgr',
]
