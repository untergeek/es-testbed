"""Component Template Entity Manager Class"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger, mapping_component, setting_component
from .entitymgr import EntityMgr

# pylint: disable=missing-docstring,too-many-arguments


class ComponentMgr(EntityMgr):
    kind = 'component'
    listname = 'component_templates'

    def __init__(self, client: Elasticsearch = None, plan: DotMap = None, autobuild: t.Optional[bool] = True):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.ComponentMgr')

    @property
    def components(self):
        retval = []
        kw = {'ilm_policy': self.plan.ilm_policies[-1], 'rollover_alias': self.plan.rollover_alias}
        retval.append(setting_component(**kw))
        retval.append(mapping_component())
        return retval

    @property
    def logdisplay(self) -> str:
        return 'component template'

    def setup(self):
        for component in self.components:
            es_api.put_comp_tmpl(self.client, self.name, component)
            if not es_api.exists(self.client, self.kind, self.name):
                raise ResultNotExpected(f'Unable to verify creation of component template {self.name}')
            self.appender(self.name)
            self.logger.info('Created component template: "%s"', self.last)
        self.logger.info('Successfully created all component templates.')
        self.success = True
