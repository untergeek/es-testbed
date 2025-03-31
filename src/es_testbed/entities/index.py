"""Index Entity Class"""

import typing as t
import logging
from os import getenv
import tiered_debug as debug
from elasticsearch8.exceptions import BadRequestError
from es_wait import Exists, IlmPhase, IlmStep
from es_wait.exceptions import EsWaitFatal, EsWaitTimeout
from es_testbed.defaults import (
    PAUSE_DEFAULT,
    PAUSE_ENVVAR,
    TIMEOUT_DEFAULT,
    TIMEOUT_ENVVAR,
)
from es_testbed.exceptions import TestbedFailure
from es_testbed.entities.entity import Entity
from es_testbed.helpers.es_api import snapshot_name
from es_testbed.helpers.utils import mounted_name, prettystr
from es_testbed.ilm import IlmTracker

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

PAUSE_VALUE = float(getenv(PAUSE_ENVVAR, default=PAUSE_DEFAULT))
TIMEOUT_VALUE = float(getenv(TIMEOUT_ENVVAR, default=TIMEOUT_DEFAULT))

logger = logging.getLogger(__name__)


class Index(Entity):
    """Index Entity Class"""

    def __init__(
        self,
        client: 'Elasticsearch',
        name: t.Union[str, None] = None,
        snapmgr=None,
        policy_name: str = None,
    ):
        debug.lv2('Initializing Index entity object...')
        super().__init__(client=client, name=name)
        self.policy_name = policy_name
        self.ilm_tracker = None
        self.snapmgr = snapmgr
        debug.lv3('Index entity object initialized')

    @property
    def _get_target(self) -> str:
        target = None
        phases = self.ilm_tracker.policy_phases
        curr = self.ilm_tracker.explain.phase
        if not bool(('cold' in phases) or ('frozen' in phases)):
            debug.lv3(f'ILM Policy for "{self.name}" has no cold/frozen phases')
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

    def _add_snap_step(self) -> None:
        debug.lv2('Starting method...')
        debug.lv4('Getting snapshot name for tracking...')
        snapname = snapshot_name(self.client, self.name)
        debug.lv5(f'Snapshot {snapname} backs {self.name}')
        self.snapmgr.add_existing(snapname)
        debug.lv3('Exiting method')

    def _ilm_step(self) -> None:
        """Subroutine for waiting for an ILM step to complete"""
        debug.lv2('Starting method...')
        step = {
            'phase': self.ilm_tracker.explain.phase,
            'action': self.ilm_tracker.explain.action,
            'name': self.ilm_tracker.explain.step,
        }
        debug.lv5(f'{self.name}: Current Step: {step}')
        step = IlmStep(
            self.client, pause=PAUSE_VALUE, timeout=TIMEOUT_VALUE, name=self.name
        )
        try:
            debug.lv4('TRY: Waiting for ILM step to complete...')
            self._wait_try(step.wait)
        except TestbedFailure as err:
            logger.error(err.message)
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception: {prettystr(err)}')
            raise err
        debug.lv3('Exiting method')

    def _wait_try(self, func: t.Callable) -> None:
        """Wait for an es-wait function to complete"""
        debug.lv2('Starting method...')
        try:
            debug.lv4('TRY: To run func()...')
            func()
        except EsWaitFatal as wait:
            # EsWaitFatal indicates we had more than the allowed number of exceptions
            msg = f'{wait.message}. Elapsed time: {wait.elapsed}. Errors: {wait.errors}'
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception: {prettystr(wait)}')
            raise TestbedFailure(msg) from wait
        except EsWaitTimeout as wait:
            # EsWaitTimeout indicates we hit the timeout
            msg = f'{wait.message}. Total elapsed time: {wait.elapsed}.'
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception: {prettystr(wait)}')
            raise TestbedFailure(msg) from wait
        except Exception as err:
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception: {prettystr(err)}')
            raise TestbedFailure(f'General Exception caught: {prettystr(err)}') from err
        debug.lv3('Exiting method')

    def _mounted_step(self, target: str) -> str:
        debug.lv2('Starting method...')
        try:
            debug.lv4(f'TRY: Moving "{self.name}" to ILM phase "{target}"')
            self.ilm_tracker.advance(phase=target)
        except BadRequestError as err:
            logger.critical(f'err: {prettystr(err)}')
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception: {prettystr(err)}')
            raise err  # Re-raise after logging
        # At this point, it's "in" a searchable tier, but the index name hasn't
        # changed yet
        newidx = mounted_name(self.name, target)
        debug.lv3(f'Waiting for ILM phase change to complete. New index: {newidx}')
        wait_kwargs = {
            'name': newidx,
            'kind': 'index',
            'pause': PAUSE_VALUE,
            'timeout': TIMEOUT_VALUE,
        }

        test = Exists(self.client, **wait_kwargs)
        debug.lv5(f'Exists response: {prettystr(test)}')
        try:
            debug.lv4(f'TRY: Waiting for "{newidx}" to exist...')
            self._wait_try(test.wait)
        except TestbedFailure as err:
            logger.error(err.message)
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception: {prettystr(err)}')
            raise err

        # Update the name and run
        debug.lv3(f'Updating self.name from "{self.name}" to "{newidx}"...')
        self.name = newidx

        # Wait for the ILM steps to complete
        debug.lv3('Waiting for the ILM steps to complete...')
        self._ilm_step()

        # Track the new index
        debug.lv3(f'Switching to track "{newidx}" as self.name...')
        self.track_ilm(newidx)
        debug.lv3('Exiting method')

    def manual_ss(self, scheme: t.Dict[str, t.Any]) -> None:
        """
        If we are NOT using ILM but have specified searchable snapshots in the plan
        entities
        """
        debug.lv2('Starting method...')
        if 'target_tier' in scheme and scheme['target_tier'] in ['cold', 'frozen']:
            debug.lv3('Manually mounting as searchable snapshot...')
            debug.lv5(
                f'Mounting "{self.name}" as searchable snapshot '
                f'({scheme["target_tier"]})'
            )
            self.snapmgr.add(self.name, scheme['target_tier'])
            # Replace self.name with the renamed name
            self.name = mounted_name(self.name, scheme['target_tier'])
        debug.lv3('Exiting method')

    def mount_ss(self, scheme: dict) -> None:
        """If the index is planned to become a searchable snapshot, we do that now"""
        debug.lv2('Starting method...')
        debug.lv3(f'Checking if "{self.name}" should be a searchable snapshot')
        if self.am_i_write_idx:
            debug.lv5(
                f'"{self.name}" is the write_index. Cannot mount as searchable '
                f'snapshot'
            )
            debug.lv3('Exiting method')
            return
        if not self.policy_name:  # If we have this, chances are we have a policy
            debug.lv3(f'No ILM policy for "{self.name}". Trying manual...')
            self.manual_ss(scheme)
            debug.lv3('Exiting method')
            return
        phase = self.ilm_tracker.next_phase
        debug.lv5(f'Next phase for "{self.name}" = {phase}')
        current = self.ilm_tracker.explain.phase
        debug.lv5(f'Current phase for "{self.name}" = {current}')
        if current == 'new':
            # This is a problem. We need to be in 'hot', with rollover completed.
            debug.lv3(
                f'Our index is still in phase "{current}"!. '
                f'We need it to be in "{phase}"'
            )

            phasenext = IlmPhase(
                self.client,
                pause=PAUSE_VALUE,
                timeout=TIMEOUT_VALUE,
                name=self.name,
                phase=self.ilm_tracker.next_phase,
            )
            try:
                debug.lv4('TRY: Waiting for ILM phase to complete...')
                self._wait_try(phasenext.wait)
            except TestbedFailure as err:
                logger.error(err.message)
                debug.lv3('Exiting method, raising exception')
                debug.lv5(f'Exception: {prettystr(err)}')
                raise err
        target = self._get_target
        debug.lv5(f'Target phase for "{self.name}" = {target}')
        if current != target:
            debug.lv5(f'Current ({current}) and target ({target}) mismatch')
            debug.lv5('Waiting for ILM step to complete...')
            self.ilm_tracker.wait4complete()
            # Because the step is completed, we must now update OUR tracker to
            # reflect the updated ILM Explain information
            debug.lv5('Updating ILM tracker...')
            self.ilm_tracker.update()

            # ILM snapshot mount phase. The biggest pain of them all...
            debug.lv3(f'Moving "{self.name}" to ILM phase "{target}"')
            self._mounted_step(target)
            debug.lv3(f'ILM advance to phase "{target}" completed')

            # Record the snapshot in our tracker
            debug.lv5('Adding snapshot step to snapmgr...')
            self._add_snap_step()
        debug.lv3('Exiting method')

    def track_ilm(self, name: str) -> None:
        """
        Get ILM phase information and put it in self.ilm_tracker
        Name as an arg makes it configurable
        """
        debug.lv2('Starting method...')
        if self.policy_name:
            debug.lv5(f'ILM policy name: {self.policy_name}')
            debug.lv5(f'Creating ILM tracker for "{name}"')
            self.ilm_tracker = IlmTracker(self.client, name)
            debug.lv5(f'Updating ILM tracker for "{name}"')
            self.ilm_tracker.update()
        else:
            debug.lv5('No ILM policy name. Skipping ILM tracking')
        debug.lv3('Exiting method')
