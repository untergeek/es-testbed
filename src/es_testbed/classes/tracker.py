"""Tracker Class Definition"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.helpers.utils import getlogger
from .entitymgrs import ComponentMgr, DataStreamMgr, IlmMgr, IndexMgr, SnapshotMgr, TemplateMgr

# pylint: disable=missing-docstring,too-many-instance-attributes

TYPEMAP = {'indices': IndexMgr, 'data_stream': DataStreamMgr}


class Tracker:
    """Object for tracking entities created in TestBed"""

    def __init__(self, client: Elasticsearch = None, plan: DotMap = None, autobuild: t.Optional[bool] = False):
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
        kw = {'client': self.client, 'plan': self.plan}
        self.ilm_policies = IlmMgr(**kw)
        self.components = ComponentMgr(**kw)
        self.templates = TemplateMgr(**kw)
        self.snapshots = SnapshotMgr(**kw)
        etype = TYPEMAP[self.plan.type]
        self.entities = etype(**kw, snapmgr=self.snapshots)

    def teardown(self):
        # These are ordered this way on purpose
        self.entities.teardown()
        self.snapshots.teardown()
        self.templates.teardown()
        self.components.teardown()
        self.ilm_policies.teardown()
