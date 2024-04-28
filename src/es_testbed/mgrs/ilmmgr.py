"""ILM Policy Entity Manager Class"""

import typing as t
from .entitymgr import EntityMgr
from ..exceptions import ResultNotExpected
from ..helpers.es_api import exists, put_ilm
from ..helpers.utils import build_ilm_policy, getlogger

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

# pylint: disable=missing-docstring


class IlmMgr(EntityMgr):
    kind = 'ilm'
    listname = 'ilm_policies'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
        autobuild: t.Optional[bool] = True,
    ):
        self.logger = getlogger('es_testbed.IlmMgr')
        super().__init__(client=client, plan=plan, autobuild=autobuild)

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
            self.logger.info('Successfully created ILM policy: %s', self.last)
        else:
            self.appender(None)  # This covers self.plan.ilm_policies[-1]
            self.logger.info('No ILM policy created.')
        self.success = True
