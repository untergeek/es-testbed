"""Make class import nicer"""

from .componentmgr import ComponentMgr
from .data_streammgr import DataStreamMgr
from .entitymgr import EntityMgr
from .ilmmgr import IlmMgr
from .indexmgr import IndexMgr
from .snapshotmgr import SnapshotMgr
from .templatemgr import TemplateMgr

__all__ = ['ComponentMgr', 'DataStreamMgr', 'EntityMgr', 'IlmMgr', 'IndexMgr', 'SnapshotMgr', 'TemplateMgr']
