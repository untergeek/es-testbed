"""Index Template Entity Manager Class"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .entitymgr import EntityMgr

# pylint: disable=missing-docstring,too-many-arguments


class TemplateMgr(EntityMgr):
    kind = 'template'
    listname = 'index_templates'

    def __init__(
        self, client: t.Union[Elasticsearch, None] = None, plan: DotMap = None, autobuild: t.Optional[bool] = True
    ):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.TemplateMgr')

    @property
    def logdisplay(self) -> str:
        return 'index template'

    @property
    def patterns(self) -> t.Sequence[str]:
        _ = []
        _.append(f"{self.get_pattern('index')}*")
        _.append(f"{self.get_pattern('data_stream')}*")
        return _

    def get_pattern(self, kind: str) -> str:
        return f'{self.plan.prefix}-{self.ident(dkey=kind)}-{self.plan.uniq}'

    def setup(self):
        ds = {} if self.plan.type == 'data_stream' else None
        es_api.put_idx_tmpl(
            self.client, self.name, self.patterns, components=self.plan.component_templates, data_stream=ds
        )
        if not es_api.exists(self.client, self.kind, self.name):
            raise ResultNotExpected(f'Unable to verify creation of index template {self.name}')
        self.appender(self.name)
        self.logger.debug('Successfully created index template: %s', self.last)
        self.success = True
