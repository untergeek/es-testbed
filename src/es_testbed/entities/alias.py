"""Alias Entity Class"""

import typing as t
from .entity import Entity
from ..helpers.es_api import resolver, rollover
from ..helpers.utils import getlogger


if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

# pylint: disable=missing-docstring,too-many-arguments


class Alias(Entity):

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
    ):
        self.logger = getlogger('es_testbed.Alias')
        super().__init__(client=client, name=name)

    def rollover(self) -> None:
        rollover(self.client, self.name)

    def verify(self, index_list: t.Sequence[str]) -> bool:
        retval = False
        res = resolver(self.client, self.name)
        for idx, alias in enumerate(res['aliases']):
            if alias['name'] == self.name:
                self.logger.debug('Confirm match of alias %s at index %s', alias, idx)
            else:
                continue
            if alias['indices'] == index_list:
                self.logger.debug(
                    'Confirm match of indices backed by alias %s', self.name
                )
                retval = True
                break
        return retval
