"""Tracker Class Definition"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.helpers.utils import getlogger
from .entitymgrs import (
    ComponentMgr, DatastreamMgr, IlmMgr, IndexMgr, SnapshotMgr, TemplateMgr)
from  .testplan import TestPlan

# pylint: disable=missing-docstring,too-many-instance-attributes

class Tracker:
    """Object for tracking entities created in TestBed"""
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = False,
        ):
        """Initialize"""
        self.logger = getlogger('es_testbed.Tracker')
        self.client = client
        self.plan = plan
        self.ilm_policies = None
        self.components = None
        self.templates = None
        self.entities = None
        self.snapshots = None
        if autobuild:
            self.setup()

    def setup(self):
        # Build IlmBuilder from self.plan.ilm
        self.ilm_policies = IlmMgr(client=self.client, plan=self.plan)
        self.logger.debug('ilm_policies = %s', self.ilm_policies.entity_list)
        alias = None
        if self.plan.rollover_alias:
            alias = f'{self.plan.prefix}-idx-{self.plan.uniq}'
        self.components = ComponentMgr(
            client=self.client,
            plan=self.plan,
            ilm_policy=self.ilm_policies.last,
            rollover_alias=alias
        )
        self.templates = TemplateMgr(
            client=self.client,
            plan=self.plan,
            components=self.components.entity_list,
            is_ds=self.plan.type == 'datastreams'
        )
        self.snapshots = SnapshotMgr(
            client=self.client,
            plan=self.plan
        )
        if self.plan.type == 'indices':
            self.logger.debug('Tracker.entities will be IndexMgr')
            etype = IndexMgr
        elif self.plan.type == 'datastreams':
            self.logger.debug('Tracker.entities will be DatastreamMgr')
            etype = DatastreamMgr
        self.entities = etype(
            client=self.client,
            plan=self.plan,
            snapmgr=self.snapshots,
            policy_name=self.ilm_policies.last
        )

    def teardown(self):
        self.entities.teardown()
        self.snapshots.teardown()
        self.templates.teardown()
        self.components.teardown()
        self.ilm_policies.teardown()
