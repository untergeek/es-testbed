"""Index Entity Manager Class"""
import typing as t
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger, setting_component
from .entitymgr import EntityMgr
from .snapshotmgr import SnapshotMgr
from ..entities import Alias, Index
from ..testplan import TestPlan

# pylint: disable=missing-docstring,too-many-arguments,broad-exception-caught

class IndexMgr(EntityMgr):
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = True,
            snapmgr: SnapshotMgr = None,
            policy_name: str = None,
        ):
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.kind = 'index'
        self.logger = getlogger('es_testbed.IndexMgr')
        self.snapmgr = snapmgr
        self.policy_name = policy_name
        self.alias = None # Only used for tracking the rollover alias
        self.doc_incr = 0
        if self.autobuild:
            self.setup()

    @property
    def aliasname(self) -> str:
        return f'{self.plan.prefix}-{self.ident()}-{self.plan.uniq}'

    @property
    def indexlist(self) -> t.Sequence[str]:
        return [x.name for x in self.entity_list]

    def add(self, value) -> None:
        # In this case, value is a single array element from plan.entities
        self.logger.debug('Creating index: %s', value)
        es_api.create_index(self.client, value)
        self.filler(value)
        self.track_index(value)

    def add_rollover(self) -> None:
        settings = setting_component(
            ilm_policy=self.policy_name, rollover_alias=self.aliasname)['settings']
        aliascfg = {self.aliasname: {'is_write_index': True}}
        for scheme in self.plan.entities:
            if not self.entity_list:
                self.logger.debug('No indices created yet. Starting with a rollover alias index...')
                es_api.create_index(
                    self.client, self.name, aliases=aliascfg, settings=settings)
                self.logger.debug('Created %s with rollover alias %s', self.name, self.aliasname)
                self.track_alias()
            else:
                self.alias.rollover()
                if self.policy_name:
                    self.logger.debug('Going to wait now...')
                    self.last.ilm_tracker.wait4complete()
                    self.logger.debug('The wait is over!')
            self.filler(scheme)
            self.track_index(self.name)
        created = [x.name for x in self.entity_list]
        self.logger.debug('Created indices: %s', created)
        if not self.alias.verify(created):
            self.logger.error('Unable to confirm rollover of alias "%s" was successfully executed')

    def filler(self, scheme) -> None:
        """If the scheme from the TestPlan says to write docs, do it"""
        # scheme is a single array element from plan.entities
        self.logger.debug('Adding docs to %s', self.name)
        if scheme['docs'] > 0:
            es_api.fill_index(
                self.client,
                name=self.name,
                count=scheme['docs'],
                start_num=self.doc_incr,
                match=scheme['match']
            )
        self.doc_incr += scheme['docs']

    def searchable(self) -> None:
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.entities):
            self.entity_list[idx].mount_ss(scheme)

    def setup(self) -> None:
        self.logger.debug('Beginning setup...')
        if self.plan.rollover_alias:
            self.logger.debug('rollover_alias is True...')
            self.add_rollover()
        else:
            for scheme in self.plan.entities:
                self.add(scheme)
        self.searchable()
        self.logger.info('Successfully created indices: %s', self.indexlist)
        self.success = True

    def track_alias(self) -> None:
        self.logger.debug('Tracking alias: %s', self.aliasname)
        self.alias = Alias(client=self.client, name=self.aliasname)

    def track_index(self, name: str) -> None:
        self.logger.debug('Tracking index: %s', name)
        entity = Index(
            client=self.client,
            name=name,
            snapmgr=self.snapmgr,
            policy_name=self.policy_name
        )
        self.entity_list.append(entity)
