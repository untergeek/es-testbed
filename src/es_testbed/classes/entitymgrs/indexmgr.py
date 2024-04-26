"""Index Entity Manager Class"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.helpers import es_api
from es_testbed.helpers.utils import getlogger, setting_component
from .entitymgr import EntityMgr
from .snapshotmgr import SnapshotMgr
from ..entities import Alias, Index

# pylint: disable=missing-docstring,too-many-arguments,broad-exception-caught,too-many-instance-attributes


class IndexMgr(EntityMgr):
    kind = 'index'
    listname = 'indices'

    def __init__(
        self,
        client: Elasticsearch = None,
        plan: DotMap = None,
        autobuild: t.Optional[bool] = True,
        snapmgr: SnapshotMgr = None,
    ):
        self.doc_incr = 0
        self.snapmgr = snapmgr
        self.alias = None  # Only used for tracking the rollover alias
        super().__init__(client=client, plan=plan, autobuild=autobuild)
        self.logger = getlogger('es_testbed.IndexMgr')

    @property
    def indexlist(self) -> t.Sequence[str]:
        return [x.name for x in self.entity_list]

    @property
    def policy_name(self) -> str:
        if len(self.plan.ilm_policies) > 0:
            return self.plan.ilm_policies[-1]
        return None

    def _rollover_path(self) -> None:
        if not self.entity_list:
            kw = {'ilm_policy': self.policy_name, 'rollover_alias': self.plan.rollover_alias}
            cfg = setting_component(**kw)['settings']
            acfg = {self.plan.rollover_alias: {'is_write_index': True}}
            self.logger.debug('No indices created yet. Starting with a rollover alias index...')
            es_api.create_index(self.client, self.name, aliases=acfg, settings=cfg)
            self.logger.debug('Created %s with rollover alias %s', self.name, self.plan.rollover_alias)
            self.track_alias()
        else:
            self.alias.rollover()
            if self.policy_name:  # We have an ILM policy
                self.logger.debug('Going to wait now...')
                self.last.ilm_tracker.wait4complete()
                self.logger.debug('The wait is over!')

    def add(self, value) -> None:
        # In this case, value is a single array element from plan.entities
        self.logger.debug('Creating index: %s', value)
        es_api.create_index(self.client, value)

    def add_indices(self) -> None:
        for scheme in self.plan.entities:
            if self.plan.rollover_alias:
                self._rollover_path()
            else:
                self.add(self.name)
            self.filler(scheme)
            self.track_index(self.name)
        self.logger.debug('Created indices: %s', self.indexlist)
        if self.plan.rollover_alias:
            if not self.alias.verify(self.indexlist):
                self.logger.error('Unable to confirm rollover of alias "%s" was successfully executed')

    def filler(self, scheme) -> None:
        """If the scheme from the TestPlan says to write docs, do it"""
        # scheme is a single array element from plan.entities
        self.logger.debug('Adding docs to %s', self.name)
        if scheme['docs'] > 0:
            es_api.fill_index(
                self.client, name=self.name, count=scheme['docs'], start_num=self.doc_incr, match=scheme['match']
            )
        self.doc_incr += scheme['docs']

    def searchable(self) -> None:
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.entities):
            old = self.entity_list[idx].name
            self.entity_list[idx].mount_ss(scheme)
            new = self.entity_list[idx].name
            # Replace the old index name in self.failsafe with the new one at the same list position
            pos = [i for i, value in enumerate(self.failsafe) if value == old]
            self.failsafe[pos[0]] = new

    def setup(self) -> None:
        self.logger.debug('Beginning setup...')
        if self.plan.rollover_alias:
            self.logger.debug('rollover_alias is True...')
        self.add_indices()
        self.searchable()
        self.logger.info('Successfully created indices: %s', self.indexlist)
        self.success = True

    def track_alias(self) -> None:
        self.logger.debug('Tracking alias: %s', self.plan.rollover_alias)
        self.alias = Alias(client=self.client, name=self.plan.rollover_alias)

    def track_index(self, name: str) -> None:
        self.logger.debug('Tracking index: %s', name)
        entity = Index(client=self.client, name=name, snapmgr=self.snapmgr, policy_name=self.policy_name)
        self.failsafe.append(name)
        self.entity_list.append(entity)
