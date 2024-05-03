"""Alias Entity Class"""

import typing as t
import logging
from es_testbed.entities.entity import Entity
from es_testbed.helpers.es_api import resolver, rollover
from es_testbed.helpers.utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger(__name__)

# pylint: disable=missing-docstring,too-many-arguments


class Alias(Entity):

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        super().__init__(client=client, name=name)

    def rollover(self) -> None:
        rollover(self.client, self.name)

    def verify(self, index_list: t.Sequence[str]) -> bool:
        retval = False
        res = resolver(self.client, self.name)
        for idx, alias in enumerate(res['aliases']):
            if alias['name'] == self.name:
                logger.debug(
                    'Confirm list index position [%s] match of alias %s',
                    idx,
                    prettystr(alias),
                )
            else:
                continue
            if alias['indices'] == index_list:
                logger.debug('Confirm match of indices backed by alias %s', self.name)
                retval = True
                break
        return retval
