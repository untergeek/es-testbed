"""data_stream Entity Class"""

# pylint: disable=C0115,C0116,R0902,R0904,R0913,R0917
import typing as t
import logging
import tiered_debug as debug
from es_testbed.entities.alias import Alias
from es_testbed.helpers.es_api import get_backing_indices

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class DataStream(Alias):
    """data_stream Entity Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        debug.lv2('Initializing DataStream entity object...')
        super().__init__(client=client, name=name)
        self.index_tracker = []
        self.alias = None
        debug.lv3('DataStream entity object initialized')

    @property
    def backing_indices(self):
        """Return the list of backing indices for the data_stream"""
        debug.lv2('Starting method...')
        retval = get_backing_indices(self.client, self.name)
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def verify(self, index_list: t.Sequence[str]) -> bool:
        """Verify that the backing indices match ``index_list``"""
        debug.lv2('Starting method...')
        retval = False
        if self.backing_indices == index_list:
            debug.lv3(f'Confirm match of data_stream "{self.name}" backing indices')
            retval = True
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval
