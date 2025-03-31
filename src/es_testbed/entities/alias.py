"""Alias Entity Class"""

# pylint: disable=C0115,C0116,R0902,R0904,R0913,R0917
import typing as t
import logging
import tiered_debug as debug
from es_testbed.entities.entity import Entity
from es_testbed.helpers.es_api import resolver, rollover
from es_testbed.helpers.utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)


class Alias(Entity):

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        debug.lv2('Initializing Alias entity object...')
        super().__init__(client=client, name=name)
        debug.lv3('Alias entity object initialized')

    def rollover(self) -> None:
        """Rollover the alias"""
        debug.lv2('Starting method...')
        rollover(self.client, self.name)
        debug.lv3('Exiting method')

    def verify(self, index_list: t.Sequence[str]) -> bool:
        debug.lv2('Starting method...')
        retval = False
        res = resolver(self.client, self.name)
        debug.lv5(f'resolver response: {prettystr(res)}')
        for idx, alias in enumerate(res['aliases']):
            if alias['name'] == self.name:
                debug.lv3(
                    f'Confirm list index position [{idx}] match of alias '
                    f'{prettystr(alias)}'
                )
            else:
                continue
            if alias['indices'] == index_list:
                debug.lv3(f'Confirm match of indices backed by alias {self.name}')
                retval = True
                break
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval
