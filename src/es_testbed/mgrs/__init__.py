"""Module to make class imports nicer for the mgrs package."""

from .component import ComponentMgr
from .data_stream import DataStreamMgr
from .ilm import IlmMgr
from .index import IndexMgr
from .snapshot import SnapshotMgr
from .template import TemplateMgr

__all__ = [
    'ComponentMgr',
    'DataStreamMgr',
    'IlmMgr',
    'IndexMgr',
    'SnapshotMgr',
    'TemplateMgr',
]
