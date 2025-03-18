"""ILM Defining Class"""

import typing as t
import logging
from os import getenv
import time
from dotmap import DotMap
from es_wait import IlmPhase, IlmStep
from es_wait.exceptions import EsWaitFatal, EsWaitTimeout
from es_testbed.defaults import (
    PAUSE_ENVVAR,
    PAUSE_DEFAULT,
    TIMEOUT_DEFAULT,
    TIMEOUT_ENVVAR,
)
from es_testbed.exceptions import (
    NameChanged,
    ResultNotExpected,
    TestbedMisconfig,
    TestbedFailure,
)
from es_testbed.helpers.es_api import get_ilm_phases, ilm_explain, ilm_move, resolver
from es_testbed.helpers.utils import prettystr

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

PAUSE_VALUE = float(getenv(PAUSE_ENVVAR, default=PAUSE_DEFAULT))
TIMEOUT_VALUE = float(getenv(TIMEOUT_ENVVAR, default=TIMEOUT_DEFAULT))

logger = logging.getLogger('es_testbed.IlmTracker')

# ## Example ILM explain output
# {
#     'action': 'complete',
#     'action_time_millis': 0,
#     'age': '5.65m',
#     'index': 'INDEX_NAME',
#     'index_creation_date_millis': 0,
#     'lifecycle_date_millis': 0,
#     'managed': True,
#     'phase': 'hot',
#     'phase_execution': {
#         'modified_date_in_millis': 0,
#         'phase_definition': {
#             'actions': {
#                 'rollover': {
#                     'max_age': 'MAX_AGE',
#                     'max_primary_shard_docs': 1000,
#                     'max_primary_shard_size': 'MAX_SIZE',
#                     'min_docs': 1
#                 }
#             },
#             'min_age': '0ms'
#         },
#         'policy': 'POLICY_NAME',
#         'version': 1
#     },
#     'phase_time_millis': 0,
#     'policy': 'POLICY_NAME',
#     'step': 'complete',
#     'step_time_millis': 0,
#     'time_since_index_creation': '5.65m'
# }


class IlmTracker:
    """ILM Phase Tracking Class"""

    def __init__(self, client: 'Elasticsearch', name: str):
        self.client = client
        self.name = self.resolve(name)  # A single index name
        self._explain = DotMap(self.get_explain_data())
        self._phases = get_ilm_phases(self.client, self._explain.policy)

    @property
    def explain(self) -> DotMap:
        """Return the current stored value of ILM Explain"""
        return self._explain

    @property
    def policy_phases(self) -> t.Sequence[str]:
        """Return a list of phases in the ILM policy"""
        return list(self._phases.keys())

    def _log_phase(self, phase: str) -> None:
        logger.debug(f'ILM Explain Index: {self._explain.index}')
        logger.info(f'Index "{self.name}" now on phase "{phase}"')

    def _phase_wait(
        self, phase: str, pause: float = PAUSE_VALUE, timeout: float = TIMEOUT_VALUE
    ) -> None:
        """Wait until the new phase shows up in ILM Explain"""
        kw = {'name': self.name, 'phase': phase, 'pause': pause, 'timeout': timeout}
        phasechk = IlmPhase(self.client, **kw)
        try:
            phasechk.wait()
        except EsWaitFatal as wait:
            msg = (
                f'{wait.message}. Total elapsed time: {wait.elapsed}. '
                f'Errors: {prettystr(wait.errors)}'
            )
            logger.error(msg)
            raise TestbedFailure(msg) from wait
        except EsWaitTimeout as wait:
            msg = f'{wait.message}. Total elapsed time: {wait.elapsed}.'
            logger.error(msg)
            raise TestbedFailure(msg) from wait

    def _ssphz(self, phase: str) -> bool:
        """Return True if the phase is for searchable snapshots (> 'warm')"""
        return bool(self.pnum(phase) > self.pnum('warm'))

    def advance(
        self,
        phase: t.Optional[str] = None,
        action: t.Optional[str] = None,
        name: t.Optional[str] = None,
    ) -> None:
        """Advance index to next ILM phase"""
        if self._explain.phase == 'delete':
            logger.warning('Already on "delete" phase. No more phases to advance')
            return

        logger.debug(f'current_step: {prettystr(self.current_step())}')
        next_step = self.next_step(phase, action=action, name=name)
        logger.debug(f'next_step: {prettystr(next_step)}')
        if self._explain.phase == 'new' and phase == 'hot':
            # It won't be for very long.
            self._phase_wait('hot')
            time.sleep(1)  # Just to make sure the new index is ready

        # Regardless of the remaining phases, the current phase steps must be
        # complete before proceeding with ilm_move
        self.update()
        self.wait4complete()
        self.update()

        # We could have arrived with it hot, but incomplete
        if phase == 'hot':
            self._log_phase(phase)
            # we've advanced to our target phase, and all steps are completed
            return

        # Remaining phases could be warm through frozen
        if self._explain.phase != phase:
            logger.debug(f'Current phase: {self.explain.phase}')
            logger.debug(f'Advancing to "{phase}" phase...')
            # We will only wait for steps to complete for the hot and warm tiers
            wait4steps = not self._ssphz(phase)
            ilm_move(self.client, self.name, self.current_step(), next_step)
            # Let the cluster catch up before proceeding.
            time.sleep(1)  # Just to make sure the cluster state has gelled
            self._phase_wait(phase)
            # If cold or frozen, we can return now. We let the calling function
            # worry about the weird name changing behavior of searchable mounts
            if wait4steps:
                self.update()
                logger.debug(f'Waiting for "{phase}" phase steps to complete...')
                self.wait4complete()
                self.update()
            self._log_phase(phase)
        else:
            logger.debug(f'Already on "{phase}" phase. No need to advance')
            logger.debug(f'current_step: {prettystr(self.current_step())}')

    def current_step(self) -> t.Dict[str, str]:
        """Return the current ILM step information

        There is a disconnect between what the ILM Explain API returns and what
        the ILM Move API expects. The former returns a 'step' key, while the
        latter expects a 'name' key. This property returns a dictionary for
        use with the ILM Move API, so that the 'step' key is renamed to 'name'.
        """

        return {
            'phase': self._explain.phase,
            'action': self._explain.action,
            'name': self._explain.step,
        }

    def get_explain_data(self) -> t.Dict:
        """Get the ILM explain data and return it"""
        try:
            return ilm_explain(self.client, self.name)
        except NameChanged as err:
            logger.debug('Passing along upstream exception...')
            raise err
        except ResultNotExpected as err:
            msg = f'Unable to get ilm_explain results. Error: {prettystr(err)}'
            logger.critical(msg)
            raise err

    def next_phase(self) -> str:
        """Return the next phase in the index's ILM journey"""
        retval = None
        if self._explain.phase == 'delete':
            logger.warning('Already on "delete" phase. No more phases to advance')
        else:
            curr = self.pnum(self._explain.phase)  # A numeric representation
            # A list of any remaining phases in the policy with a higher number than
            # the current
            remaining = [
                self.pnum(x) for x in self.policy_phases if self.pnum(x) > curr
            ]
            if remaining:  # If any:
                retval = self.pname(remaining[0])
                # Get the phase name from the number stored in the first element
        return retval

    def next_step(
        self,
        phase: t.Optional[str] = None,
        action: t.Optional[str] = None,
        name: t.Optional[str] = None,
    ) -> t.Dict[str, str]:
        """Determine the next ILM step based on the current phase, action, and name

        There is a disconnect between what the ILM Explain API returns and what
        the ILM Move API expects. The former returns a 'step' key, while the
        latter expects a 'name' key. This property returns a dictionary for
        use with the ILM Move API, so that the 'step' key is renamed to 'name'.
        """
        err1 = bool((action is not None) and (name is None))
        err2 = bool((action is None) and (name is not None))
        if err1 or err2:
            msg = 'If either action or name is specified, both must be'
            logger.critical(msg)
            raise TestbedMisconfig(msg)
        if not phase:
            logger.debug('No phase specified. Using next_phase')
            phase = self.next_phase()
            logger.debug(f'next_phase: {phase}')
        retval = {'phase': phase}
        if action:
            retval['action'] = action
            retval['name'] = name
        return retval

    def pnum(self, phase: str) -> int:
        """Map a phase name to a phase number"""
        _ = {'new': 0, 'hot': 1, 'warm': 2, 'cold': 3, 'frozen': 4, 'delete': 5}
        return _[phase]

    def pname(self, num: int) -> str:
        """Map a phase number to a phase name"""
        _ = {0: 'new', 1: 'hot', 2: 'warm', 3: 'cold', 4: 'frozen', 5: 'delete'}
        return _[num]

    def resolve(self, name: str) -> str:
        """Resolve that we have an index and NOT an alias or a datastream"""
        res = resolver(self.client, name)
        if len(res['aliases']) > 0 or len(res['data_streams']) > 0:
            msg = f'{name} is not an index: {res}'
            logger.critical(msg)
            raise ResultNotExpected(msg)
        if len(res['indices']) > 1:
            msg = f'{name} resolved to multiple indices: {prettystr(res["indices"])}'
            logger.critical(msg)
            raise ResultNotExpected(msg)
        return res['indices'][0]['name']

    def update(self) -> None:
        """Update self._explain with the latest from :py:meth:`get_explain_data`"""
        try:
            self._explain = DotMap(self.get_explain_data())
        except NameChanged as err:
            logger.debug('Passing along upstream exception...')
            raise err

    def wait4complete(self) -> None:
        """Subroutine for waiting for an ILM step to complete"""
        phase_action = bool(self._explain.action == 'complete')
        phase_step = bool(self._explain.step == 'complete')
        if bool(phase_action and phase_step):
            logger.debug(
                f'{self.name}: Current step complete: {prettystr(self.current_step())}'
            )
            return
        logger.debug(
            f'{self.name}: Current step not complete. {prettystr(self.current_step())}'
        )
        kw = {'name': self.name, 'pause': PAUSE_VALUE, 'timeout': TIMEOUT_VALUE}
        step = IlmStep(self.client, **kw)
        try:
            step.wait()
            logger.debug('ILM Step successful. The wait is over')
            time.sleep(1)  # Just to make sure the cluster state has gelled
        except EsWaitFatal as wait:
            msg = (
                f'{wait.message}. Total elapsed time: {wait.elapsed}. '
                f'Errors: {prettystr(wait.errors)}'
            )
            logger.error(msg)
            raise TestbedFailure(msg) from wait
        except EsWaitTimeout as wait:
            msg = f'{wait.message}. Total elapsed time: {wait.elapsed}.'
            logger.error(msg)
            raise TestbedFailure(msg) from wait
