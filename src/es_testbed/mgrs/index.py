"""Index Entity Manager Class"""

import typing as t
import logging
from importlib import import_module
from ..debug import debug, begin_end
from ..entities import Alias, Index
from ..es_api import create_index, fill_index
from ..utils import prettystr
from .entity import EntityMgr
from .snapshot import SnapshotMgr

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
        client: t.Optional['Elasticsearch'] = None,
        plan: t.Optional['DotMap'] = None,
        snapmgr: t.Optional[SnapshotMgr] = None,
    ):
        self.snapmgr = snapmgr
        self.alias = None  # Only used for tracking the rollover alias
        debug.lv2('Initializing IndexMgr object...')
        super().__init__(client=client, plan=plan)
        debug.lv3('IndexMgr object initialized')

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

    @begin_end()
    def _rollover_path(self) -> None:
        """This is the execution path for rollover indices"""
        if not self.entity_list:
            acfg = {self.plan.rollover_alias: {'is_write_index': True}}
            debug.lv5(f'Creating index with config: {acfg}')
            create_index(self.client, self.name, aliases=acfg)
            self.track_alias()
        else:
            self.alias.rollover()
            debug.lv5('Rolled over index with alias')
            if self.policy_name:  # We have an ILM policy
                kw = {'phase': 'hot', 'action': 'complete', 'name': 'complete'}
                debug.lv5(f'Advancing ILM policy with config: {kw}')
                self.last.ilm_tracker.advance(**kw)

    @begin_end()
    def add(self, value) -> None:
        """Create a single index"""
        debug.lv1(f'Creating index: "{value}"')
        create_index(self.client, value)

    @begin_end()
    def add_indices(self) -> None:
        """Add indices according to plan"""
        mod = import_module(f'{self.plan.modpath}.functions')
        func = getattr(mod, 'doc_generator')
        for scheme in self.plan.index_buildlist:
            if self.plan.rollover_alias:
                self._rollover_path()
            else:
                self.add(self.name)
            # self.filler(scheme)
            fill_index(
                self.client,
                name=self.name,
                doc_generator=func,
                options=scheme['options'],
            )
            self.track_index(self.name)
        debug.lv2(f'Created indices: {prettystr(self.indexlist)}')
        if self.plan.rollover_alias:
            if not self.alias.verify(self.indexlist):
                logger.error(
                    f'Unable to confirm rollover of alias '
                    f'"{self.plan.rollover_alias}" was successful'
                )

    @begin_end()
    def searchable(self) -> None:
        """If the indices were marked as searchable snapshots, we do that now"""
        for idx, scheme in enumerate(self.plan.index_buildlist):
            if scheme['target_tier'] in ['cold', 'frozen']:
                self.entity_list[idx].mount_ss(scheme)

    @begin_end()
    def setup(self) -> None:
        """Setup the entity manager"""
        debug.lv5(f'PLAN: {prettystr(self.plan.toDict())}')
        if self.plan.rollover_alias:
            debug.lv3('rollover_alias is True...')
        self.add_indices()
        self.searchable()
        logger.info(f'Successfully created indices: {prettystr(self.indexlist)}')

    @begin_end()
    def track_alias(self) -> None:
        """Track a rollover alias"""
        debug.lv3(f'Tracking alias: {self.plan.rollover_alias}')
        self.alias = Alias(client=self.client, name=self.plan.rollover_alias)

    @begin_end()
    def track_index(self, name: str) -> None:
        """Track an index and append that tracking entity to entity_list"""
        debug.lv3(f'Tracking index: {name}')
        entity = Index(
            client=self.client,
            name=name,
            snapmgr=self.snapmgr,
            policy_name=self.policy_name,
        )
        entity.track_ilm(self.name)
        self.entity_list.append(entity)
