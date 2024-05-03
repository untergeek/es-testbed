"""Index Entity Class"""

import typing as t
import logging
from os import getenv
from elasticsearch8.exceptions import BadRequestError
from es_wait import Exists, IlmPhase, IlmStep
from es_wait.exceptions import IlmWaitError
from es_testbed.defaults import (
    PAUSE_DEFAULT,
    PAUSE_ENVVAR,
    TIMEOUT_DEFAULT,
    TIMEOUT_ENVVAR,
)
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
        super().__init__(client=client, name=name)
        self.policy_name = policy_name
        self.ilm_tracker = None
        self.snapmgr = snapmgr

    @property
    def _get_target(self) -> str:
        target = None
        phases = self.ilm_tracker.policy_phases
        curr = self.ilm_tracker.explain.phase
        if not bool(('cold' in phases) or ('frozen' in phases)):
            logger.info('ILM Policy for "%s" has no cold/frozen phases', self.name)
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
        logger.debug('Getting snapshot name for tracking...')
        snapname = snapshot_name(self.client, self.name)
        logger.debug('Snapshot %s backs %s', snapname, self.name)
        self.snapmgr.add_existing(snapname)

    def _ilm_step(self) -> None:
        """Subroutine for waiting for an ILM step to complete"""
        step = {
            'phase': self.ilm_tracker.explain.phase,
            'action': self.ilm_tracker.explain.action,
            'name': self.ilm_tracker.explain.step,
        }
        logger.debug('%s: Current Step: %s', self.name, step)
        step = IlmStep(
            self.client, pause=PAUSE_VALUE, timeout=TIMEOUT_VALUE, name=self.name
        )
        try:
            step.wait_for_it()
            logger.debug('ILM Step successful. The wait is over')
        except KeyError as exc:
            logger.error('KeyError: The index name has changed: "%s"', exc)
            raise exc
        except BadRequestError as exc:
            logger.error('Index not found')
            raise exc
        except IlmWaitError as exc:
            logger.error('Other IlmWait error encountered: "%s"', exc)
            raise exc

    def _mounted_step(self, target: str) -> str:
        try:
            self.ilm_tracker.advance(phase=target)
        except BadRequestError as err:
            logger.critical('err: %s', prettystr(err))
            raise BadRequestError from err
        # At this point, it's "in" a searchable tier, but the index name hasn't
        # changed yet
        newidx = mounted_name(self.name, target)
        logger.debug('Waiting for ILM phase change to complete. New index: %s', newidx)
        kwargs = {
            'name': newidx,
            'kind': 'index',
            'pause': PAUSE_VALUE,
            'timeout': TIMEOUT_VALUE,
        }
        test = Exists(self.client, **kwargs)
        test.wait_for_it()

        # Update the name and run
        logger.debug('Updating self.name from "%s" to "%s"...', self.name, newidx)
        self.name = newidx

        # Wait for the ILM steps to complete
        logger.debug('Waiting for the ILM steps to complete...')
        self._ilm_step()

        # Track the new index
        logger.debug('Switching to track "%s" as self.name...', newidx)
        self.track_ilm(newidx)

    # This is maybe unnecessary. This is for progressing ILM, e.g. from
    # hot -> warm -> cold -> frozen (and even through delete).
    #
    # def _loop_until_target(self) -> None:
    #     current, target = self.phase_tuple
    #     while current != target:
    #         logger.debug(
    #             'Attempting to move %s to ILM phase %s', self.name, target
    #         )
    #         self.ilm_tracker.advance(phase=target)
    #         # At this point, it's "in" a searchable tier, but the index name hasn't
    #         # changed yet
    #         newidx = mounted_name(self.name, target)
    #         logger.debug(
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
    #         logger.info('ILM advance to phase %s completed', target)
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
        logger.debug('Checking if "%s" should be a searchable snapshot', self.name)
        if self.am_i_write_idx:
            logger.debug(
                '"%s" is the write_index. Cannot mount as searchable snapshot',
                self.name,
            )
            return
        if not self.policy_name:  # If we have this, chances are we have a policy
            logger.debug('No ILM policy for "%s". Trying manual...', self.name)
            self.manual_ss(scheme)
            return
        phase = self.ilm_tracker.next_phase
        current = self.ilm_tracker.explain.phase
        if current == 'new':
            # This is a problem. We need to be in 'hot', with rollover completed.
            logger.debug(
                'Our index is still in phase "%s"!. We need it to be in "%s"',
                current,
                phase,
            )

            phasenext = IlmPhase(
                self.client,
                pause=PAUSE_VALUE,
                timeout=TIMEOUT_VALUE,
                name=self.name,
                phase=self.ilm_tracker.next_phase,
            )
            phasenext.wait_for_it()
        target = self._get_target
        if current != target:
            logger.debug('Current (%s) and target (%s) mismatch', current, target)
            self.ilm_tracker.wait4complete()
            # Because the step is completed, we must now update OUR tracker to
            # reflect the updated ILM Explain information
            self.ilm_tracker.update()

            # ILM snapshot mount phase. The biggest pain of them all...
            logger.debug('Moving "%s" to ILM phase "%s"', self.name, target)
            self._mounted_step(target)
            logger.info('ILM advance to phase "%s" completed', target)

            # Record the snapshot in our tracker
            self._add_snap_step()

    def track_ilm(self, name: str) -> None:
        """
        Get ILM phase information and put it in self.ilm_tracker
        Name as an arg makes it configurable
        """
        if self.policy_name:
            self.ilm_tracker = IlmTracker(self.client, name)
            self.ilm_tracker.update()
