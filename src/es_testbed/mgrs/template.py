"""Index Template Entity Manager Class"""

import typing as t
import logging
import tiered_debug as debug
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers.es_api import exists, put_idx_tmpl
from es_testbed.mgrs.entity import EntityMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class TemplateMgr(EntityMgr):
    """Index Template entity manager"""

    kind = 'template'
    listname = 'index_templates'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
    ):
        debug.lv2('Initializing TemplateMgr object...')
        super().__init__(client=client, plan=plan)
        debug.lv3('TemplateMgr object initialized')

    @property
    def patterns(self) -> t.Sequence[str]:
        """Return the list of index patterns associated with this template"""
        _ = []
        _.append(f"{self.get_pattern('index')}*")
        _.append(f"{self.get_pattern('data_stream')}*")
        return _

    def get_pattern(self, kind: str) -> str:
        """Return the a formatted index search pattern string"""
        debug.lv2('Starting method...')
        retval = f'{self.plan.prefix}-{self.ident(dkey=kind)}-{self.plan.uniq}'
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def setup(self) -> None:
        """Setup the entity manager"""
        debug.lv2('Starting method...')
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
        debug.lv3(f'Successfully created index template: {self.last}')
        debug.lv3('Exiting method')
