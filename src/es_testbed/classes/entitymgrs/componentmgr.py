"""Component Template Entity Manager Class"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger, mapping_component, setting_component
from .entitymgr import EntityMgr
from ..testplan import TestPlan

# pylint: disable=missing-docstring,too-many-arguments

class ComponentMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = True,
            ilm_policy: t.Dict[str, t.Any] = None,
            rollover_alias: str = None,
        ):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.ComponentMgr')
        self.kind = 'component'
        self.components = []
        self.components.append(
            setting_component(
                ilm_policy=ilm_policy, rollover_alias=rollover_alias)
            )
        self.components.append(mapping_component())
        if self.autobuild:
            self.setup()

    def setup(self):
        for component in self.components:
            es_api.put_comp_tmpl(self.client, self.name, component)
            if not es_api.exists(self.client, 'component', self.name):
                raise ResultNotExpected(
                    f'Unable to verify creation of component template {self.name}')
            self.entity_list.append(self.name)
            self.logger.info('Created component template: "%s"', self.last)
        self.logger.info('Successfully created all component templates.')
        self.success = True

    def teardown(self):
        self.logger.info('Cleaning up all component templates...')
        components = ','.join(self.entity_list)
        es_api.delete(self.client, 'component', components)
