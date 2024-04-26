"""ILM Policy Entity Manager Class"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import build_ilm_policy, getlogger
from .entitymgr import EntityMgr

# pylint: disable=missing-docstring


class IlmMgr(EntityMgr):
    kind = 'ilm'
    listname = 'ilm_policies'

    def __init__(self, client: Elasticsearch = None, plan: DotMap = None, autobuild: t.Optional[bool] = True):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.IlmMgr')

    @property
    def logdisplay(self) -> str:
        return 'ILM policy'

    def get_policy(self):
        d = self.plan.ilm
        kwargs = {
            'tiers': d.tiers,
            'forcemerge': d.forcemerge,
            'max_num_segments': d.max_num_segments,
            'repository': self.plan.repository,
        }
        return build_ilm_policy(**kwargs)

    def setup(self):
        if self.plan.ilm.enabled:
            self.plan.ilm.policy = self.get_policy()
            es_api.put_ilm(self.client, self.name, policy=self.plan.ilm.policy)
            # Verify existence
            if not es_api.exists(self.client, 'ilm', self.name):
                raise ResultNotExpected(f'Unable to verify creation of ilm policy {self.name}')
            # This goes first because the length of entity_list determines the suffix
            self.appender(self.name)
            self.logger.info('Successfully created ILM policy: %s', self.last)
        else:
            self.appender(None)  # This covers self.plan.ilm_policies[-1]
            self.logger.info('No ILM policy created.')
        self.success = True
