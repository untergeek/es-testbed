"""Index Entity Class"""

import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from . import Alias

# pylint: disable=missing-docstring,too-many-arguments


class DataStream(Alias):

    def __init__(self, client: Elasticsearch = None, name: str = None, autobuild: t.Optional[bool] = True):
        super().__init__(client=client, name=name, autobuild=autobuild)
        self.logger = getlogger('es_testbed.Data_Stream')
        self.index_tracker = []
        self.alias = None

    @property
    def backing_indices(self):
        return es_api.get_backing_indices(self.client, self.name)

    def verify(self, index_list: t.Sequence[str]) -> bool:
        retval = False
        if self.backing_indices == index_list:
            self.logger.debug('Confirm match of data_stream "%s" backing indices', self.name)
            retval = True
        return retval
