"""Entity Class Definition"""
import logging
from elasticsearch8 import Elasticsearch
from es_testbed.classes.ilmbuilder import IlmBuilder
from es_testbed.classes.testplan import TestPlan
from es_testbed.classes.tracker import Tracker
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import mapping_component, mounted_name, setting_component

# pylint: disable=missing-docstring

class EntityMgr:
    def __init__(self, client: Elasticsearch):
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.entity_list = []

    def get(self):
        return self.entity_list

    @property
    def last(self):
        """Return the most recently appended item"""
        return self.entity_list[-1]

    def add(self, value):
        self.entity_list.append(value)

    def setup(self):
        pass

    def teardown(self):
        pass

class SnapshotMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch,
            namemgr: Tracker.NameMgr,
            plan: TestPlan=None,
            tier: str=None,
            autobuild: bool=False
        ):
        super().__init__(client)
        self.namemgr = namemgr
        self.plan = plan
        self.tier = tier
        if autobuild:
            pass

    def add(self, value):
        # In this override, value is the index name or index pattern
        es_api.do_snap(
            self.client,
            self.plan.ilm.repository,
            self.namemgr.snapshot,
            value,
            tier=self.tier
        )
        self.entity_list.append(self.namemgr.snapshot)

    def setup(self):
        pass

    def teardown(self):
        for snapshot in self.entity_list:
            es_api.delete_snapshot(self.client, self.plan.ilm.repository, snapshot)

class ComponentMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch,
            namemgr: Tracker.NameMgr,
            ilm_policy: str=None,
            rollover_alias: str=None,
            autobuild: bool=True
        ):
        super().__init__(client)
        self.namemgr = namemgr
        self.components = []
        self.components.append(setting_component(
            ilm_policy=ilm_policy, rollover_alias=rollover_alias))
        self.components.append(mapping_component())
        if autobuild:
            self.setup()

    def setup(self):
        for component in self.components:
            es_api.put_comp_tmpl(self.client, self.namemgr.component, component)
            self.add(self.namemgr.component)

    def teardown(self):
        for component in self.entity_list:
            es_api.delete_component(self.client, component)

class IlmMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch,
            namemgr: Tracker.NameMgr,
            ilm: IlmBuilder=None,
            autobuild: bool=True
        ):
        super().__init__(client)
        self.namemgr = namemgr
        self.ilm = ilm
        if autobuild:
            self.setup()

    def setup(self):
        if self.ilm:
            es_api.put_ilm(self.client, self.namemgr.ilm, policy=self.ilm.policy)
            self.add(self.namemgr.ilm)

    def teardown(self):
        for policy in self.entity_list:
            es_api.delete_ilm(self.client, policy)

class IndexMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch,
            namemgr: Tracker.NameMgr,
            snapmgr: SnapshotMgr,
            plan: TestPlan=None,
            autobuild: bool=True):
        super().__init__(client)
        self.namemgr = namemgr
        self.snapmgr = snapmgr
        self.plan = plan
        self.doc_incr = 0
        if autobuild:
            self.setup()

    def add(self, value):
        # In this case, value is a single array element from plan.entities
        es_api.create_index(self.client, self.namemgr.index)
        self.entity_list.append(self.namemgr.index)
        self.filler(value)

    def add_rollover(self):
        settings = setting_component(
            ilm_policy=self.namemgr.ilm, rollover_alias=self.namemgr.alias)['settings']
        aliases = {self.namemgr.alias: {'is_write_index': True}}
        for scheme in self.plan.entities:
            if not self.entity_list:
                es_api.create_index(
                    self.client, self.namemgr.index, aliases=aliases, settings=settings)
            else:
                self.rollover(self.namemgr.alias)
            self.entity_list.append(self.namemgr.index)
            self.filler(scheme)
        res = es_api.resolver(self.client, self.namemgr.alias)
        if len(res['indices']) != len(self.entity_list):
            self.logger.critical('We are missing some indices!')
            self.logger.critical('Resolver: %s', res['indices'])
            self.logger.critical('Tracked internally: %s', self.entity_list)
        # At the end here, we should verify that all indices created are in self.entity_list

    def filler(self, scheme):
        """If the scheme from the TestPlan says to write docs, do it"""
        # scheme is a single array element from plan.entities
        if scheme['docs'] > 0:
            es_api.fill_index(
                self.client,
                name=self.last, # Grab our most recently created entity name
                count=scheme['docs'],
                start_num=self.doc_incr,
                match=scheme['match']
            )
        self.doc_incr += scheme['docs']

    def rollover(self, name):
        """Trigger a rollover of an alias or datastream identified by name"""
        if name:
            es_api.rollover(self.client, name)

    def searchable(self):
        """If the indices were marked as searchable snapshots, we do that now"""
        write_index = es_api.get_write_index(self.client, self.namemgr.alias)
        for idx, scheme in enumerate(self.plan.entities):
            if 'searchable' in scheme:
                origname = self.entity_list[idx]
                if origname != write_index:
                    # We need to set where to mount it before we can add it
                    self.snapmgr.tier = scheme['searchable']
                    self.snapmgr.add(origname)
                    # Replace the entry for origname in self.last with the renamed name
                    self.entity_list[idx] = mounted_name(origname, scheme['searchable'])
                else:
                    self.logger.debug(
                        '%s is the active index. Not able to make a searchable snapshot', origname)

    def setup(self):
        if self.plan.rollover_alias:
            self.add_rollover()
        else:
            for scheme in self.plan.entities:
                self.add(scheme)
        self.searchable()

    def teardown(self):
        for index in self.entity_list:
            es_api.delete_index(self.client, index)

class DatastreamMgr(IndexMgr):
    def __init__(
            self,
            client: Elasticsearch,
            namemgr: Tracker.NameMgr,
            snapmgr: SnapshotMgr,
            plan: TestPlan=None,
            autobuild: bool=True
        ):
        super().__init__(client, namemgr, snapmgr, plan)
        self.indices = []
        if autobuild:
            self.setup()

    def add(self, value):
        es_api.create_datastream(self.client, value)
        self.entity_list.append(value)

    @property
    def backing_indices(self):
        return es_api.get_backing_indices(self.client, self.last)

    def searchable(self):
        write_index = es_api.get_ds_current(self.client, self.last)
        for idx, scheme in enumerate(self.plan.entities):
            if 'searchable' in scheme:
                origname = self.indices[idx]
                if origname != write_index:
                    # We need to set where to mount it before we can add it
                    self.snapmgr.tier = scheme['searchable']
                    self.snapmgr.add(origname)
                    # Replace the entry for origname in self.last with the renamed name
                    self.indices[idx] = mounted_name(origname, scheme['searchable'])
                else:
                    self.logger.debug(
                        '%s is the active index. Not able to make a searchable snapshot', origname)

    def setup(self):
        name = None
        for scheme in self.plan.entities:
            if not self.entity_list:
                self.add(self.namemgr.datastream)
                name = self.last
            else:
                self.rollover(name)
            self.filler(scheme)
        self.indices = self.backing_indices()

    def teardown(self):
        for index in self.entity_list:
            es_api.delete_index(self.client, index)

class TemplateMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch,
            namemgr: Tracker.NameMgr,
            componentmgr: ComponentMgr=None,
            is_ds: bool=False,
            autobuild: bool=True
        ):
        super().__init__(client)
        self.namemgr = namemgr
        self.componentmgr = componentmgr
        self.ds = {} if is_ds else None
        if autobuild:
            self.setup()

    def setup(self):
        es_api.put_idx_tmpl(
            self.client,
            self.namemgr.template,
            self.namemgr.patterns,
            components=self.componentmgr.entity_list,
            data_stream=self.ds
        )
        self.add(self.namemgr.template)

    def teardown(self):
        for template in self.entity_list:
            es_api.delete_template(self.client, template)
