"""ILM Policy Entity Manager Class"""

import typing as t
import logging
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers.es_api import exists, put_ilm
from es_testbed.helpers.utils import build_ilm_policy
from es_testbed.mgrs.entity import EntityMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class IlmMgr(EntityMgr):
    """Index Lifecycle Policy Entity Manager"""

    kind = 'ilm'
    listname = 'ilm_policies'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
    ):
        super().__init__(client=client, plan=plan)

    def get_policy(self) -> t.Dict:
        """Return the configured ILM policy"""
        d = self.plan.ilm
        kwargs = {
            'tiers': d.tiers,
            'forcemerge': d.forcemerge,
            'max_num_segments': d.max_num_segments,
            'repository': self.plan.repository,
        }
        return build_ilm_policy(**kwargs)

    def setup(self) -> None:
        """Setup the entity manager"""
        logger.debug('Starting IlmMgr setup...')
        if self.plan.ilm.enabled:
            self.plan.ilm.policy = self.get_policy()
            put_ilm(self.client, self.name, policy=self.plan.ilm.policy)
            # Verify existence
            if not exists(self.client, 'ilm', self.name):
                raise ResultNotExpected(
                    f'Unable to verify creation of ilm policy {self.name}'
                )
            # This goes first because the length of entity_list determines the suffix
            self.appender(self.name)
            logger.info('Successfully created ILM policy: %s', self.last)
            logger.debug(self.client.ilm.get_lifecycle(name=self.last))
        else:
            self.appender(None)  # This covers self.plan.ilm_policies[-1]
            logger.info('No ILM policy created.')
