"""Base Entity Class"""

import typing as t
from ..helpers.es_api import find_write_index, get_ds_current, resolver

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

# pylint: disable=missing-docstring,too-many-arguments,R0903


class Entity:

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        self.client = client
        self.name = name  # This will change with entity name changes
        self.aka = []  # Aliases, in other words

    @property
    def am_i_write_idx(self) -> bool:
        if self.name.startswith('.ds-'):  # Datastream
            ds = resolver(self.client, self.name)['indices'][0]['data_stream']
            return bool(self.name == get_ds_current(self.client, ds))
        return bool(self.name == find_write_index(self.client, self.name))
