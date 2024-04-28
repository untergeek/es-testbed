"""Index Entity Class"""

import typing as t
from .alias import Alias
from ..helpers.es_api import get_backing_indices
from ..helpers.utils import getlogger

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

# pylint: disable=missing-docstring,too-many-arguments


class DataStream(Alias):

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        self.logger = getlogger('es_testbed.DataStream')
        super().__init__(client=client, name=name)
        self.index_tracker = []
        self.alias = None

    @property
    def backing_indices(self):
        return get_backing_indices(self.client, self.name)

    def verify(self, index_list: t.Sequence[str]) -> bool:
        retval = False
        if self.backing_indices == index_list:
            self.logger.debug(
                'Confirm match of data_stream "%s" backing indices', self.name
            )
            retval = True
        return retval
