"""ILM Policy Entity Manager Class"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.exceptions import ResultNotExpected
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger
from .entitymgr import EntityMgr
from ..ilm import IlmBuilder
from ..testplan import TestPlan
# pylint: disable=missing-docstring

class IlmMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = True,
        ):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.IlmMgr')
        self.kind = 'ilm'
        self.ilm = False
        # If ILM was configured in the plan settings, plan.ilm should be an IlmBuilder instance
        # If not, we won't end up using ILM
        if isinstance(self.plan, TestPlan):
            self.logger.debug('We have a plan object')
            self.logger.debug('Our plan object contains: %s', self.plan.asdict)
            if isinstance(self.plan.ilm, IlmBuilder):
                self.logger.debug('Our plan object is an IlmBuilder class instance')
                self.ilm = self.plan.ilm
                self.logger.debug('Our plan is: %s', self.ilm.asdict)
        else:
            self.logger.debug('We have no plan object')
        if self.autobuild:
            self.setup()

    @property
    def logdisplay(self) -> str:
        return 'ILM policy'

    def setup(self):
        if isinstance(self.ilm, IlmBuilder):
            es_api.put_ilm(self.client, self.name, policy=self.ilm.policy)
            # Verify existence
            if not es_api.exists(self.client, 'ilm', self.name):
                raise ResultNotExpected(
                    f'Unable to verify creation of ilm policy {self.name}')
            self.entity_list.append(self.name)
            self.logger.info('Successfully created ILM policy: %s', self.last)
        else:
            self.entity_list.append(None)
            self.logger.info('No ILM policy created.')
        self.success = True
