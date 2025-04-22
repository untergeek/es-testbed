"""Component Template Entity Manager Class"""

import typing as t
import logging
from importlib import import_module
from ..debug import debug, begin_end
from ..es_api import exists, put_comp_tmpl
from ..exceptions import ResultNotExpected
from ..utils import prettystr
from .entity import EntityMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class ComponentMgr(EntityMgr):
    """Component Template Entity Manager Class"""

    kind = "component"
    listname = "component_templates"

    def __init__(
        self,
        client: t.Union["Elasticsearch", None] = None,
        plan: t.Union["DotMap", None] = None,
    ):
        debug.lv2("Initializing ComponentMgr object...")
        super().__init__(client=client, plan=plan)
        debug.lv3("ComponentMgr object initialized")

    @property
    @begin_end()
    def components(self) -> t.Sequence[t.Dict]:
        """Return a list of component template dictionaries"""
        retval = []
        preset = import_module(f"{self.plan.modpath}.definitions")
        val = preset.settings()
        if self.plan.ilm_policies[-1]:
            val["settings"]["index.lifecycle.name"] = self.plan.ilm_policies[-1]
            if self.plan.rollover_alias:
                val["settings"][
                    "index.lifecycle.rollover_alias"
                ] = self.plan.rollover_alias
        retval.append(val)
        retval.append(preset.mappings())
        return retval

    @begin_end()
    def setup(self) -> None:
        """Setup the entity manager"""
        for component in self.components:
            put_comp_tmpl(self.client, self.name, component)
            if not exists(self.client, self.kind, self.name):
                raise ResultNotExpected(
                    f"Unable to verify creation of component template {self.name}"
                )
            self.appender(self.name)
        logger.info(
            f"Successfully created all component templates: "
            f"{prettystr(self.entity_list)}"
        )
