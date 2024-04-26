"""Alias Entity Class"""

import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.helpers.utils import getlogger
from es_testbed.helpers import es_api
from .entity import Entity

# pylint: disable=missing-docstring,too-many-arguments


class Alias(Entity):

    def __init__(self, client: Elasticsearch, name: str = None, autobuild: t.Optional[bool] = True):
        super().__init__(client=client, name=name, autobuild=autobuild)
        self.logger = getlogger('es_testbed.Alias')

    def rollover(self) -> None:
        es_api.rollover(self.client, self.name)

    def verify(self, index_list: t.Sequence[str]) -> bool:
        retval = False
        res = es_api.resolver(self.client, self.name)
        for idx, alias in enumerate(res['aliases']):
            if alias['name'] == self.name:
                self.logger.debug('Confirm match of alias %s at index %s', alias, idx)
            else:
                continue
            if alias['indices'] == index_list:
                self.logger.debug('Confirm match of indices backed by alias %s', self.name)
                retval = True
                break
        return retval
