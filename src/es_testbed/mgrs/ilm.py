"""ILM Policy Entity Manager Class"""

import typing as t
import logging
from ..debug import debug, begin_end
from ..es_api import exists, put_ilm
from ..exceptions import ResultNotExpected
from ..utils import build_ilm_policy
from .entity import EntityMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class IlmMgr(EntityMgr):
    """Index Lifecycle Policy Entity Manager"""

    kind = "ilm"
    listname = "ilm_policies"

    def __init__(
        self,
        client: t.Union["Elasticsearch", None] = None,
        plan: t.Union["DotMap", None] = None,
    ):
        """Initialize the ILM policy manager"""
        debug.lv2("Initializing IlmMgr object...")
        super().__init__(client=client, plan=plan)
        debug.lv3("IlmMgr object initialized")

    @begin_end()
    def get_policy(self) -> t.Dict:
        """Return the configured ILM policy"""
        d = self.plan.ilm
        kwargs = {
            "phases": d.phases,
            "forcemerge": d.forcemerge,
            "max_num_segments": d.max_num_segments,
            "readonly": d.readonly,
            "repository": self.plan.repository,
        }
        retval = build_ilm_policy(**kwargs)
        debug.lv5(f"Return value = {retval}")
        return retval

    @begin_end()
    def setup(self) -> None:
        """Setup the entity manager"""
        if self.plan.ilm.enabled:
            if not self.plan.ilm.policy:  # If you've put a full policy there...
                self.plan.ilm.policy = self.get_policy()
            put_ilm(self.client, self.name, policy=self.plan.ilm.policy)
            # Verify existence
            if not exists(self.client, "ilm", self.name):
                raise ResultNotExpected(
                    f"Unable to verify creation of ilm policy {self.name}"
                )
            # This goes first because the length of entity_list determines the suffix
            self.appender(self.name)
            logger.info(f"Successfully created ILM policy: {self.last}")
            debug.lv5(self.client.ilm.get_lifecycle(name=self.last))
        else:
            self.appender(None)  # This covers self.plan.ilm_policies[-1]
            logger.info("No ILM policy created.")
