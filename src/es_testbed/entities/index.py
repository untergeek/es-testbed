"""Index Entity Class"""

import typing as t
from os import getenv
from es_wait import Exists
from .entity import Entity
from ..ilm import IlmTracker
from ..defaults import (
    PAUSE_DEFAULT,
    PAUSE_ENVVAR,
    TIMEOUT_DEFAULT,
    TIMEOUT_ENVVAR,
)
from ..exceptions import NameChanged, ResultNotExpected
from ..helpers.es_api import snapshot_name
from ..helpers.utils import getlogger, mounted_name

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

PAUSE_VALUE = float(getenv(PAUSE_ENVVAR, default=PAUSE_DEFAULT))
TIMEOUT_VALUE = float(getenv(TIMEOUT_ENVVAR, default=TIMEOUT_DEFAULT))

# pylint: disable=missing-docstring,too-many-arguments


class Index(Entity):

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
        snapmgr=None,
        policy_name: str = None,
    ):
        self.logger = getlogger('es_testbed.Index')
        super().__init__(client=client, name=name)
        self.policy_name = policy_name
        self.ilm_tracker = None
        self.snapmgr = snapmgr
        self.track_ilm(self.name)

    @property
    def _get_target(self) -> str:
        target = None
        phases = self.ilm_tracker.policy_phases
        curr = self.ilm_tracker.explain.phase
        if not bool(('cold' in phases) or ('frozen' in phases)):
            self.logger.info('ILM Policy for "%s" has no cold/frozen phases', self.name)
            target = curr  # Keep the same
        if bool(('cold' in phases) and ('frozen' in phases)):
            if self.ilm_tracker.pname(curr) < self.ilm_tracker.pname('cold'):
                target = 'cold'
            elif curr == 'cold':
                target = 'frozen'
            elif self.ilm_tracker.pname(curr) >= self.ilm_tracker.pname('frozen'):
                target = curr
        elif bool(('cold' in phases) and ('frozen' not in phases)):
            target = 'cold'
        elif bool(('cold' not in phases) and ('frozen' in phases)):
            target = 'frozen'
        return target

    @property
    def phase_tuple(self) -> t.Tuple[str, str]:
        """Return the current phase and the target phase as a Tuple"""
        return self.ilm_tracker.explain.phase, self._get_target

    # This is maybe unnecessary. This is for progressing ILM, e.g. from
    # hot -> warm -> cold -> frozen (and even through delete).
    #
    # def _loop_until_target(self) -> None:
    #     current, target = self.phase_tuple
    #     while current != target:
    #         self.logger.debug(
    #             'Attempting to move %s to ILM phase %s', self.name, target
    #         )
    #         self.ilm_tracker.advance(phase=target)
    #         # At this point, it's "in" a searchable tier, but the index name hasn't
    #         # changed yet
    #         newidx = mounted_name(self.name, target)
    #         self.logger.debug(
    #             'Waiting for ILM phase change to complete. New index: %s', newidx
    #         )
    #         kwargs = {
    #             'name': newidx,
    #             'kind': 'index',
    #             'pause': PAUSE_VALUE,
    #             'timeout': TIMEOUT_VALUE,
    #         }
    #         test = Exists(self.client, **kwargs)
    #         test.wait_for_it()
    #         self.logger.info('ILM advance to phase %s completed', target)
    #         self.aka.append(self.name)  # Append the old name to the AKA list
    #         self.name = newidx
    #         self.track_ilm(self.name)  # Refresh the ilm_tracker with the new name
    #         current, target = self.phase_tuple

    def manual_ss(self, scheme) -> None:
        """
        If we are NOT using ILM but have specified searchable snapshots in the plan
        entities
        """
        if 'searchable' in scheme and scheme['searchable'] is not None:
            self.snapmgr.add(self.name, scheme['searchable'])
            # Replace self.name with the renamed name
            self.name = mounted_name(self.name, scheme['searchable'])

    def mount_ss(self, scheme: dict) -> None:
        """If the index is planned to become a searchable snapshot, we do that now"""
        self.logger.debug('Checking if %s should be a searchable snapshot', self.name)
        if self.am_i_write_idx:
            self.logger.debug(
                '%s is the write_index. Cannot mount as searchable snapshot', self.name
            )
            return
        if not self.policy_name:  # If we have this, chances are we have a policy
            self.logger.debug('No ILM policy found. Switching to manual mode')
            self.manual_ss(scheme)
            return
        current = self.ilm_tracker.explain.phase
        target = self._get_target
        if current != target:
            self.logger.debug(
                'Attempting to move %s to ILM phase %s', self.name, target
            )
            self.ilm_tracker.advance(phase=target)
            # At this point, it's "in" a searchable tier, but the index name hasn't
            # changed yet
            newidx = mounted_name(self.name, target)
            self.logger.debug(
                'Waiting for ILM phase change to complete. New index: %s', newidx
            )
            kwargs = {
                'name': newidx,
                'kind': 'index',
                'pause': PAUSE_VALUE,
                'timeout': TIMEOUT_VALUE,
            }
            test = Exists(self.client, **kwargs)
            test.wait_for_it()
            try:
                self.ilm_tracker.wait4complete()
            except NameChanged:
                try:
                    self.track_ilm(newidx)
                    self.ilm_tracker.wait4complete()
                except NameChanged as err:
                    self.logger.critical('Index name mismatch. Cannot continue')
                    raise ResultNotExpected from err
            self.logger.info('ILM advance to phase %s completed', target)
            self.logger.debug('Getting snapshot name for tracking...')
            snapname = snapshot_name(self.client, newidx)
            self.logger.debug('Snapshot %s backs %s', snapname, newidx)
            self.snapmgr.add_existing(snapname)
            self.aka.append(self.name)  # Append the old name to the AKA list
            self.name = newidx
            self.track_ilm(self.name)  # Refresh the ilm_tracker with the new index name

    def track_ilm(self, name: str) -> None:
        """
        Get ILM phase information and put it in self.ilm_tracker
        Name as an arg makes it configurable
        """
        if self.policy_name:
            self.ilm_tracker = IlmTracker(self.client, name)
            self.ilm_tracker.update()
