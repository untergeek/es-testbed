"""Component Template Entity Manager Class"""

import typing as t
from .entitymgr import EntityMgr
from ..exceptions import ResultNotExpected
from ..helpers.es_api import exists, put_comp_tmpl
from ..helpers.utils import getlogger, mapping_component, setting_component

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

# pylint: disable=missing-docstring,too-many-arguments


class ComponentMgr(EntityMgr):
    kind = 'component'
    listname = 'component_templates'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
        autobuild: t.Optional[bool] = True,
    ):
        self.logger = getlogger('es_testbed.ComponentMgr')
        super().__init__(client=client, plan=plan, autobuild=autobuild)

    @property
    def components(self) -> t.Sequence[t.Dict]:
        """Return a list of component template dictionaries"""
        retval = []
        kw = {
            'ilm_policy': self.plan.ilm_policies[-1],
            'rollover_alias': self.plan.rollover_alias,
        }
        retval.append(setting_component(**kw))
        retval.append(mapping_component())
        return retval

    def setup(self) -> None:
        """Setup the entity manager"""
        for component in self.components:
            put_comp_tmpl(self.client, self.name, component)
            if not exists(self.client, self.kind, self.name):
                raise ResultNotExpected(
                    f'Unable to verify creation of component template {self.name}'
                )
            self.appender(self.name)
        self.logger.info(
            'Successfully created all component templates: %s', self.entity_list
        )
        self.success = True
