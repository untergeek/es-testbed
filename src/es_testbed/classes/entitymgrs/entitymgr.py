"""Entity Class Definition"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.defaults import NAMEMAPPER
from es_testbed.helpers.utils import getlogger
from ..testplan import TestPlan

# pylint: disable=missing-docstring

class EntityMgr:
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = True,
        ):
        self.kind = 'entity_type'
        self.logger = getlogger('es_testbed.EntityMgr')
        self.client = client
        self.plan = plan
        self.autobuild = autobuild
        self.entity_list = []
        self.success = False

    @property
    def last(self):
        """Return the most recently appended item"""
        return self.entity_list[-1]
    @property
    def name(self) -> str:
        return f'{self.plan.prefix}-{self.ident()}-{self.plan.uniq}{self.suffix}'
    @property
    def suffix(self):
        return f'-{len(self.entity_list) + 1:06}'

    def ident(self, dkey=None):
        if not dkey:
            dkey=self.kind
        return NAMEMAPPER[dkey]

    def setup(self):
        pass

    def teardown(self):
        pass
