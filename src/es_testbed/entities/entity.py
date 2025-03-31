"""Entity Parent Class"""

# pylint: disable=R0903
import typing as t
import tiered_debug as debug
from es_testbed.helpers.es_api import find_write_index, get_ds_current, resolver

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch


class Entity:
    """Entity Parent Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        self.client = client
        self.name = name  # This will change with entity name changes

    @property
    def am_i_write_idx(self) -> bool:
        """
        Determine if self.name is the write index for either a rollover alias or a
        data_stream
        """
        debug.lv2('Starting method...')
        if self.name.startswith('.ds-'):  # Datastream
            debug.lv2('Datastream detected')
            ds = resolver(self.client, self.name)['indices'][0]['data_stream']
            debug.lv5(f'resolver response: {ds}')
            retval = bool(self.name == get_ds_current(self.client, ds))
            debug.lv3('Exiting method, returning value')
            debug.lv5(f'Value = {retval}')
            return retval
        debug.lv2('Rollover alias detected')
        retval = bool(self.name == find_write_index(self.client, self.name))
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def set_debug_tier(self, tier: int) -> None:
        """
        Set the debug tier globally for this module
        """
        debug.set_level(tier)
