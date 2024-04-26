"""Base Entity Class"""

import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger

# pylint: disable=missing-docstring,too-many-arguments


class Entity:

    def __init__(self, client: Elasticsearch, name: str = None, autobuild: t.Optional[bool] = True):
        self.client = client
        self.name = name  # This will change with entity name changes
        self.aka = []  # Aliases, in other words
        self.logger = getlogger('es_testbed.Entity')
        if autobuild:
            self.setup()

    @property
    def am_i_write_idx(self) -> bool:
        if self.name.startswith('.ds-'):  # Datastream
            ds = es_api.resolver(self.client, self.name)['indices'][0]['data_stream']
            return bool(self.name == es_api.get_ds_current(self.client, ds))
        return bool(self.name == es_api.find_write_index(self.client, self.name))

    def setup(self):
        pass

    def teardown(self):
        pass
