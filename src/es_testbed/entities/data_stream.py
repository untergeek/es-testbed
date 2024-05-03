"""data_stream Entity Class"""

import typing as t
import logging
from es_testbed.entities.alias import Alias
from es_testbed.helpers.es_api import get_backing_indices

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=missing-docstring,too-many-arguments


class DataStream(Alias):
    """data_stream Entity Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        super().__init__(client=client, name=name)
        self.index_tracker = []
        self.alias = None

    @property
    def backing_indices(self):
        """Return the list of backing indices for the data_stream"""
        return get_backing_indices(self.client, self.name)

    def verify(self, index_list: t.Sequence[str]) -> bool:
        """Verify that the backing indices match ``index_list``"""
        retval = False
        if self.backing_indices == index_list:
            logger.debug('Confirm match of data_stream "%s" backing indices', self.name)
            retval = True
        return retval
