"""Index Template Entity Manager Class"""

import typing as t
from .entitymgr import EntityMgr
from ..exceptions import ResultNotExpected
from ..helpers.es_api import exists, put_idx_tmpl
from ..helpers.utils import getlogger

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

# pylint: disable=missing-docstring,too-many-arguments


class TemplateMgr(EntityMgr):
    """Index Template entity manager"""

    kind = 'template'
    listname = 'index_templates'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
        autobuild: t.Optional[bool] = True,
    ):
        self.logger = getlogger('es_testbed.TemplateMgr')
        super().__init__(client=client, plan=plan, autobuild=autobuild)

    @property
    def patterns(self) -> t.Sequence[str]:
        """Return the list of index patterns associated with this template"""
        _ = []
        _.append(f"{self.get_pattern('index')}*")
        _.append(f"{self.get_pattern('data_stream')}*")
        return _

    def get_pattern(self, kind: str) -> str:
        """Return the a formatted index search pattern string"""
        return f'{self.plan.prefix}-{self.ident(dkey=kind)}-{self.plan.uniq}'

    def setup(self) -> None:
        """Setup the entity manager"""
        ds = {} if self.plan.type == 'data_stream' else None
        put_idx_tmpl(
            self.client,
            self.name,
            self.patterns,
            components=self.plan.component_templates,
            data_stream=ds,
        )
        if not exists(self.client, self.kind, self.name):
            raise ResultNotExpected(
                f'Unable to verify creation of index template {self.name}'
            )
        self.appender(self.name)
        self.logger.info('Successfully created index template: %s', self.last)
        self.success = True
