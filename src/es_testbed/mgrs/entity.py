"""Entity Class Definition"""

import typing as t
from es_testbed.defaults import NAMEMAPPER

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap


class EntityMgr:
    """Entity Manager Parent Class"""

    kind = 'entity_type'
    listname = 'entity_mgrs'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
    ):
        self.client = client
        self.plan = plan

    @property
    def entity_list(self) -> t.List:
        """Return the stored list of entities"""
        return self.plan[self.listname]

    @entity_list.setter
    def entity_list(self, value: t.Sequence) -> None:
        self.plan[self.listname] = value

    @property
    def entity_root(self) -> str:
        """The entity root name builder"""
        return f'{self.plan.prefix}-{self.ident()}-{self.plan.uniq}'

    @property
    def indexlist(self) -> t.Sequence[str]:
        """Empty attribute/property waiting to be overridden"""
        return []

    @property
    def last(self) -> str:
        """Return the most recently appended entity"""
        return self.entity_list[-1]

    @property
    def name(self) -> str:
        """Return the full, incrementing name of a not yet appended entity"""
        return f'{self.entity_root}{self.suffix}'

    @property
    def pattern(self) -> str:
        """Return the search pattern for the managed entity"""
        return f'*{self.entity_root}*'

    @property
    def suffix(self) -> str:
        """Return the incrementing index suffix"""
        return f'-{len(self.entity_list) + 1:06}'

    def appender(self, name: str) -> None:
        """Append an item to entity_list"""
        self.entity_list.append(name)

    def ident(self, dkey=None) -> str:
        """Get the formatted name string of the managed entity"""
        if not dkey:
            dkey = self.kind
        return NAMEMAPPER[dkey]

    def setup(self) -> None:
        """Setup the entity manager"""

    def track_index(self, name: str) -> None:
        """Track an index and append that tracking entity to entity_list"""
