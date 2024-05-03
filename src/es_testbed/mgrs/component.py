"""Component Template Entity Manager Class"""

import typing as t
import logging
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers.es_api import exists, put_comp_tmpl
from es_testbed.helpers.utils import mapping_component, prettystr, setting_component
from es_testbed.mgrs.entity import EntityMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class ComponentMgr(EntityMgr):
    """Component Template Entity Manager Class"""

    kind = 'component'
    listname = 'component_templates'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
    ):
        super().__init__(client=client, plan=plan)

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
        logger.info(
            'Successfully created all component templates: %s',
            prettystr(self.entity_list),
        )
