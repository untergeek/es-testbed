"""ILM Defining Class"""

import typing as t
import logging
from os import getenv
import time
from dotmap import DotMap
import tiered_debug as debug
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
        debug.lv2('Initializing IlmTracker object...')
        self.client = client
        self.name = self.resolve(name)  # A single index name
        self._explain = DotMap(self.get_explain_data())
        self._phases = get_ilm_phases(self.client, self._explain.policy)
        debug.lv3('IlmTracker object initialized')

    @property
    def explain(self) -> DotMap:
        """Return the current stored value of ILM Explain"""
        return self._explain

    @property
    def policy_phases(self) -> t.Sequence[str]:
        """Return a list of phases in the ILM policy"""
        return list(self._phases.keys())

    def _log_phase(self, phase: str) -> None:
        debug.lv3(f'ILM Explain Index: {self._explain.index}')
        debug.lv2(f'Index "{self.name}" now on phase "{phase}"')

    def _phase_wait(
        self, phase: str, pause: float = PAUSE_VALUE, timeout: float = TIMEOUT_VALUE
    ) -> None:
        """Wait until the new phase shows up in ILM Explain"""
        debug.lv2('Starting method...')
        kw = {'name': self.name, 'phase': phase, 'pause': pause, 'timeout': timeout}
        debug.lv5(f'Waiting for phase args = {prettystr(kw)}')
        phasechk = IlmPhase(self.client, **kw)
        try:
            debug.lv4('TRY: Waiting for ILM phase to complete')
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
        debug.lv3('Exiting method')

    def _ssphz(self, phase: str) -> bool:
        """Return True if the phase is for searchable snapshots (> 'warm')"""
        debug.lv2('Starting method...')
        retval = bool(self.pnum(phase) > self.pnum('warm'))
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def advance(
        self,
        phase: t.Optional[str] = None,
        action: t.Optional[str] = None,
        name: t.Optional[str] = None,
    ) -> None:
        """Advance index to next ILM phase"""
        debug.lv2('Starting method...')
        if self._explain.phase == 'delete':
            debug.lv1('Already on "delete" phase. No more phases to advance')
            debug.lv3('Exiting method')
            return

        debug.lv3(f'current_step: {prettystr(self.current_step())}')
        next_step = self.next_step(phase, action=action, name=name)
        debug.lv3(f'next_step: {prettystr(next_step)}')
        if self._explain.phase == 'new' and phase == 'hot':
            debug.lv3('Phase is "new" and is still advancing to "hot"')
            # It won't be for very long.
            debug.lv5('Waiting for phase to fully reach "hot"...')
            self._phase_wait('hot')
            time.sleep(1)  # Just to make sure the new index is ready

        # Regardless of the remaining phases, the current phase steps must be
        # complete before proceeding with ilm_move
        debug.lv5('Running self.update()...')
        self.update()
        debug.lv5('Running self.wait4complete()...')
        self.wait4complete()
        debug.lv5('Running self.update()...')
        self.update()

        # We could have arrived with it hot, but incomplete
        if phase == 'hot':
            self._log_phase(phase)
            debug.lv5('Phase "hot" reached, and all steps are completed')
            debug.lv3('Exiting method')
            return

        # Remaining phases could be warm through frozen
        if self._explain.phase != phase:
            debug.lv5(f'"{self._explain.phase}" != "{phase}"')
            debug.lv3(f'Current phase: {self.explain.phase}')
            debug.lv3(f'Advancing to "{phase}" phase...')
            # We will only wait for steps to complete for the hot and warm tiers
            debug.lv5('Waiting for steps for non-cold/frozen phases to complete')
            wait4steps = not self._ssphz(phase)
            debug.lv5('Running ilm_move()...')
            ilm_move(self.client, self.name, self.current_step(), next_step)
            # Let the cluster catch up before proceeding.
            time.sleep(1)  # Just to make sure the cluster state has gelled
            debug.lv5('Running self._phase_wait()...')
            self._phase_wait(phase)
            # If cold or frozen, we can return now. We let the calling function
            # worry about the weird name changing behavior of searchable mounts
            debug.lv5('Checking if wait4steps is True')
            if wait4steps:
                debug.lv5('Running self.update()...')
                self.update()
                debug.lv3(f'Waiting for "{phase}" phase steps to complete...')
                debug.lv5('Running self.wait4complete()...')
                self.wait4complete()
                debug.lv5('Running self.update()...')
                self.update()
            self._log_phase(phase)
        else:
            debug.lv3(f'Already on "{phase}" phase. No need to advance')
            debug.lv5(f'current_step: {prettystr(self.current_step())}')
        debug.lv3('Exiting method')

    def current_step(self) -> t.Dict[str, str]:
        """Return the current ILM step information

        There is a disconnect between what the ILM Explain API returns and what
        the ILM Move API expects. The former returns a 'step' key, while the
        latter expects a 'name' key. This property returns a dictionary for
        use with the ILM Move API, so that the 'step' key is renamed to 'name'.
        """
        debug.lv2('Starting method...')
        retval = {
            'phase': self._explain.phase,
            'action': self._explain.action,
            'name': self._explain.step,
        }
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {prettystr(retval)}')
        return retval

    def get_explain_data(self) -> t.Dict:
        """Get the ILM explain data and return it"""
        debug.lv2('Starting method...')
        try:
            debug.lv4('TRY: Getting ILM explain data')
            retval = ilm_explain(self.client, self.name)
            debug.lv3('Exiting method, returning value')
            debug.lv5(f'Value = {prettystr(retval)}')
            return retval
        except NameChanged as err:
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception = {prettystr(err)}')
            debug.lv3('Apparent name change. Passing along upstream exception...')
            raise err
        except ResultNotExpected as err:
            debug.lv3('Exiting method, raising exception')
            msg = f'Unable to get ilm_explain results. Error: {prettystr(err)}'
            logger.critical(msg)
            raise err

    def next_phase(self) -> str:
        """Return the next phase in the index's ILM journey"""
        debug.lv2('Starting method...')
        retval = None
        if self._explain.phase == 'delete':
            debug.lv3('Already on "delete" phase. No more phases to advance')
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
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
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
        debug.lv2('Starting method...')
        err1 = bool((action is not None) and (name is None))
        err2 = bool((action is None) and (name is not None))
        if err1 or err2:
            debug.lv3('Exiting method, raising exception')
            msg = 'If either action or name is specified, both must be'
            logger.critical(msg)
            raise TestbedMisconfig(msg)
        if not phase:
            debug.lv3('No phase specified. Using next_phase')
            phase = self.next_phase()
            debug.lv5(f'next_phase: {phase}')
        retval = {'phase': phase}
        if action:
            retval['action'] = action
            retval['name'] = name
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {prettystr(retval)}')
        return retval

    def pnum(self, phase: str) -> int:
        """Map a phase name to a phase number"""
        debug.lv2('Starting method...')
        _ = {'new': 0, 'hot': 1, 'warm': 2, 'cold': 3, 'frozen': 4, 'delete': 5}
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {_[phase]}')
        return _[phase]

    def pname(self, num: int) -> str:
        """Map a phase number to a phase name"""
        debug.lv2('Starting method...')
        _ = {0: 'new', 1: 'hot', 2: 'warm', 3: 'cold', 4: 'frozen', 5: 'delete'}
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {_[num]}')
        return _[num]

    def resolve(self, name: str) -> str:
        """Resolve that we have an index and NOT an alias or a datastream"""
        debug.lv2('Starting method...')
        res = resolver(self.client, name)
        debug.lv5(f'resolver: result = {res}')
        if len(res['aliases']) > 0 or len(res['data_streams']) > 0:
            debug.lv3('Exiting method, raising exception')
            msg = f'{name} is not an index: {res}'
            logger.critical(msg)
            raise ResultNotExpected(msg)
        if len(res['indices']) > 1:
            debug.lv3('Exiting method, raising exception')
            msg = f'{name} resolved to multiple indices: {prettystr(res["indices"])}'
            logger.critical(msg)
            raise ResultNotExpected(msg)
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {res["indices"][0]["name"]}')
        return res['indices'][0]['name']

    def set_debug_tier(self, tier: int) -> None:
        """
        Set the debug tier globally for this module
        """
        debug.set_level(tier)

    def update(self) -> None:
        """Update self._explain with the latest from :py:meth:`get_explain_data`"""
        debug.lv2('Starting method...')
        try:
            debug.lv4('TRY: self._explain = DotMap(self.get_explain_data())')
            self._explain = DotMap(self.get_explain_data())
            debug.lv5(f'Updated explain: {prettystr(self._explain)}')
        except NameChanged as err:
            debug.lv3('Exiting method, raising exception')
            debug.lv3('Passing along upstream exception...')
            debug.lv5(f'Exception = {prettystr(err)}')
            raise err
        debug.lv3('Exiting method')

    def wait4complete(self) -> None:
        """Subroutine for waiting for an ILM step to complete"""
        debug.lv2('Starting method...')
        phase_action = bool(self._explain.action == 'complete')
        phase_step = bool(self._explain.step == 'complete')
        if bool(phase_action and phase_step):
            debug.lv3(
                f'{self.name}: Current step complete: {prettystr(self.current_step())}'
            )
            debug.lv3('Exiting method')
            return
        debug.lv3(
            f'{self.name}: Current step not complete. {prettystr(self.current_step())}'
        )
        kw = {'name': self.name, 'pause': PAUSE_VALUE, 'timeout': TIMEOUT_VALUE}
        debug.lv5(f'IlmStep args = {prettystr(kw)}')
        step = IlmStep(self.client, **kw)
        try:
            debug.lv4('TRY: Waiting for ILM step to complete')
            step.wait()
            debug.lv3('ILM Step successful. The wait is over')
            time.sleep(1)  # Just to make sure the cluster state has gelled
        except EsWaitFatal as wait:
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception = {prettystr(wait)}')
            msg = (
                f'{wait.message}. Total elapsed time: {wait.elapsed}. '
                f'Errors: {prettystr(wait.errors)}'
            )
            logger.error(msg)
            raise TestbedFailure(msg) from wait
        except EsWaitTimeout as wait:
            debug.lv3('Exiting method, raising exception')
            debug.lv5(f'Exception = {prettystr(wait)}')
            msg = f'{wait.message}. Total elapsed time: {wait.elapsed}.'
            logger.error(msg)
            raise TestbedFailure(msg) from wait
        debug.lv3('Exiting method')
