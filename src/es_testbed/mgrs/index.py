"""Index Entity Manager Class"""

import typing as t
import logging
from es_testbed.entities import Alias, Index
from es_testbed.helpers.es_api import create_index, fill_index
from es_testbed.helpers.utils import prettystr, setting_component
from es_testbed.mgrs.entity import EntityMgr
from es_testbed.mgrs.snapshot import SnapshotMgr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch
    from dotmap import DotMap

logger = logging.getLogger(__name__)


class IndexMgr(EntityMgr):
    """Index Entity Manager Class"""

    kind = 'index'
    listname = 'indices'

    def __init__(
        self,
        client: t.Union['Elasticsearch', None] = None,
        plan: t.Union['DotMap', None] = None,
        snapmgr: t.Union[SnapshotMgr, None] = None,
    ):
        self.doc_incr = 0
        self.snapmgr = snapmgr
        self.alias = None  # Only used for tracking the rollover alias
        super().__init__(client=client, plan=plan)

    @property
    def indexlist(self) -> t.Sequence[str]:
        """Return a list of index names currently being managed"""
        return [x.name for x in self.entity_list]

    @property
    def policy_name(self) -> t.Union[str, None]:
        """Return the name of the ILM policy, if it exists"""
        if len(self.plan.ilm_policies) > 0:
            return self.plan.ilm_policies[-1]
        return None

    def _rollover_path(self) -> None:
        """This is the execution path for rollover indices"""
        if not self.entity_list:
            kw = {
                'ilm_policy': self.policy_name,
                'rollover_alias': self.plan.rollover_alias,
            }
            cfg = setting_component(**kw)['settings']
            acfg = {self.plan.rollover_alias: {'is_write_index': True}}
            logger.debug(
                'No indices created yet. Starting with a rollover alias index...'
            )
            create_index(self.client, self.name, aliases=acfg, settings=cfg)
            logger.debug(
                'Created %s with rollover alias %s', self.name, self.plan.rollover_alias
            )
            self.track_alias()
        else:
            self.alias.rollover()
            if self.policy_name:  # We have an ILM policy
                kw = {'phase': 'hot', 'action': 'complete', 'name': 'complete'}
                self.last.ilm_tracker.advance(**kw)

    def add(self, value) -> None:
        """Create a single index"""
        # In this case, value is a single array element from plan.entities
        logger.debug('Creating index: "%s"', value)
        create_index(self.client, value)

    def add_indices(self) -> None:
        """Add indices according to plan"""
        for scheme in self.plan.entities:
            if self.plan.rollover_alias:
                self._rollover_path()
            else:
                self.add(self.name)
            self.filler(scheme)
            self.track_index(self.name)
        logger.debug('Created indices: %s', prettystr(self.indexlist))
        if self.plan.rollover_alias:
            if not self.alias.verify(self.indexlist):
                logger.error(
                    'Unable to confirm rollover of alias "%s" was successful',
                    self.plan.rollover_alias,
                )

    def filler(self, scheme) -> None:
        """If the scheme from the TestPlan says to write docs, do it"""
        # scheme is a single array element from plan.entities
        logger.debug('Adding docs to "%s"', self.name)
        if scheme['docs'] > 0:
            fill_index(
                self.client,
                name=self.name,
                count=scheme['docs'],
                start_num=self.doc_incr,
                match=scheme['match'],
            )
        self.doc_incr += scheme['docs']

    def searchable(self) -> None:
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.entities):
            self.entity_list[idx].mount_ss(scheme)

    def setup(self) -> None:
        """Setup the entity manager"""
        logger.debug('Beginning setup...')
        logger.debug('PLAN: %s', prettystr(self.plan.toDict()))
        if self.plan.rollover_alias:
            logger.debug('rollover_alias is True...')
        self.add_indices()
        self.searchable()
        logger.info('Successfully created indices: %s', prettystr(self.indexlist))

    def track_alias(self) -> None:
        """Track a rollover alias"""
        logger.debug('Tracking alias: %s', self.plan.rollover_alias)
        self.alias = Alias(client=self.client, name=self.plan.rollover_alias)

    def track_index(self, name: str) -> None:
        """Track an index and append that tracking entity to entity_list"""
        logger.debug('Tracking index: %s', name)
        entity = Index(
            client=self.client,
            name=name,
            snapmgr=self.snapmgr,
            policy_name=self.policy_name,
        )
        entity.track_ilm(self.name)
        self.entity_list.append(entity)
