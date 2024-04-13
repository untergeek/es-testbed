"""Tracker Class Definition"""
from es_testbed.classes.entitymgr import (
    IlmMgr, ComponentMgr, TemplateMgr, IndexMgr, DatastreamMgr, SnapshotMgr)
from es_testbed.classes.testplan import TestPlan
from es_testbed.defaults import NAMEMAPPER

# pylint: disable=missing-docstring,too-few-public-methods,invalid-name,no-member

class Tracker:
    """Object for tracking entities created in TestBed"""
    def __init__(self, plan: TestPlan, autobuild: bool=False):
        """Initialize"""
        self.plan = plan
        self.namemgr = self.NameMgr(plan.prefix, plan.uniq)
        self.ilm_policies = None
        self._components = None
        self._templates = None
        self._entities = None
        self._snapshots = None
        if autobuild:
            self.setup()

    @property
    def components(self):
        return self._components.get()
    @components.setter
    def components(self, value):
        self._components.add(value)

    @property
    def entities(self):
        return self._entities.get()
    @entities.setter
    def entities(self, value):
        self._entities.add(value)

    @property
    def ilm_policies(self):
        return self._ilm_policies.get()
    @ilm_policies.setter
    def ilm_policies(self, value):
        self._ilm_policies.add(value)

    @property
    def snapshots(self):
        return self._snapshots.get()
    @snapshots.setter
    def snapshots(self, value):
        self._snapshots.add(value)

    @property
    def templates(self):
        return self._templates.get()
    @templates.setter
    def templates(self, value):
        self._templates.add(value)

    def setup(self):
        self.ilm_policies = IlmMgr(self.client, self.namemgr, self.plan.ilm)
        alias = None
        if self.plan.rollover_alias:
            alias = self.namemgr.alias
        self.components = ComponentMgr(
            self.client, self.namemgr, ilm_policy=self.ilm_policies.last,
            rollover_alias=alias, autobuild=self.autobuild
        )
        self.templates = TemplateMgr(
            self.client,
            self.namemgr,
            self.components,
            is_ds=self.plan.type == 'datastreams',
            autobuild=self.autobuild
        )
        self.snapshots = SnapshotMgr(self.client, self.namemgr, self.plan, autobuild=self.autobuild)
        if self.plan.type == 'indices':
            self.entities = IndexMgr(
                self.client, self.namemgr, self.snapshots, self.plan, autobuild=self.autobuild)
        elif self.plan.type == 'datastreams':
            self.entities = DatastreamMgr(
                self.client, self.namemgr, self.snapshots, self.plan, autobuild=self.autobuild)

    def teardown(self):
        self.entities.teardown()
        self.snapshots.teardown()
        self.components.teardown()
        self.templates.teardown()
        self.ilm_policies.teardown()

    # The Inner class here allows NameMgr to create names and suffixes, by tracking the parent
    # properties (i.e., how many entries in each list +1 represents the -000000 zero padded number)

    class Inner:
        pass

    def NameMgr(self, prefix: str, uniq: str):
        parent = self
        class NameMgr(Tracker.Inner):
            def __init__(self, prefix: str, uniq: str):
                self.parent = parent
                self.prefix = prefix
                self.uniq = uniq
                self.patterns = []
                self.patterns.append(f"{self.name('index')}*")
                self.patterns.append(f"{self.name('datastream')}*")

            @property
            def alias(self):
                return self.name('index')
            @property
            def component(self):
                return self.name('component', suffix=len(self.parent.components) + 1)
            @property
            def datastream(self):
                return self.name('datastream')
            @property
            def ilm(self):
                return self.name('ilm', suffix=len(self.parent.ilm_policies) + 1)
            @property
            def index(self):
                return self.name('index', suffix=len(self.parent.indices) + 1)
            @property
            def snapshot(self):
                return self.name('snapshot', suffix=len(self.parent.snapshots) + 1)
            @property
            def template(self):
                return self.name('template', suffix=len(self.parent.templates) + 1)

            def name(self, kind: str, suffix: str=None) -> str:
                """
                Use self.uniq as the core of all of our naming so as not to stomp on anything

                :param kind: The key to the shortened 
                :param suffix: A number to be affixed and zero padded

                :type kind: str
                :type suffix: str
                
                :returns: {self.prefix}-{pfx[kind]}-{self.uniq},
                    appends -{suffix:06} if suffix exists
                :rtype: str
                """
                name = f'{self.prefix}-{self.pfx(kind)}-{self.uniq}'
                if suffix:
                    return f'{name}-{suffix:06}'
                return f'{name}'

            def pfx(self, kind: str):
                """Return the prefix based on kind"""
                return NAMEMAPPER[kind]
        return NameMgr(prefix, uniq)
