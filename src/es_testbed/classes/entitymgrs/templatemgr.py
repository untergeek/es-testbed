"""Index Template Entity Manager Class"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .entitymgr import EntityMgr
from ..testplan import TestPlan

# pylint: disable=missing-docstring,too-many-arguments

class TemplateMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = True,
            is_ds: t.Optional[bool] = False,
            components: t.Sequence[str] = None,
        ):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.kind = 'template'
        self.logger = getlogger('es_testbed.TemplateMgr')
        self.components = components
        self.ds = {} if is_ds else None
        if self.autobuild:
            self.setup()

    def get_pattern(self, kind: str) -> str:
        return f'{self.plan.prefix}-{self.ident(dkey=kind)}-{self.plan.uniq}'

    @property
    def logdisplay(self) -> str:
        return 'index template'

    def setup(self):
        patterns = []
        patterns.append(f"{self.get_pattern('index')}*")
        patterns.append(f"{self.get_pattern('data_stream')}*")
        es_api.put_idx_tmpl(
            self.client,
            self.name,
            patterns,
            components=self.components,
            data_stream=self.ds
        )
        if not es_api.exists(self.client, 'template', self.name):
            raise ResultNotExpected(
                f'Unable to verify creation of index template {self.name}')
        self.entity_list.append(self.name)
        self.logger.debug('Successfully created index template: %s', self.last)
        self.success = True
