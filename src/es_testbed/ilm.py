"""ILM Defining Class"""

import typing as t
from os import getenv
from time import sleep
from dotmap import DotMap
from .defaults import PAUSE_ENVVAR, PAUSE_DEFAULT
from .exceptions import NameChanged, ResultNotExpected, TestbedMisconfig
from .helpers.es_api import get_ilm_phases, ilm_explain, ilm_move, resolver
from .helpers.utils import getlogger

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

PAUSE_VALUE = float(getenv(PAUSE_ENVVAR, default=PAUSE_DEFAULT))

# pylint: disable=missing-docstring

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
        self.logger = getlogger('es_testbed.IlmTracker')
        self.client = client
        self.name = self.resolve(name)  # A single index name
        self._explain = DotMap(self.get_explain_data())
        self._phases = get_ilm_phases(self.client, self._explain.policy)

    @property
    def current_step(self) -> t.Dict:
        """Return the current ILM step information"""
        return {
            'phase': self._explain.phase,
            'action': self._explain.action,
            'name': self._explain.step,
        }

    @property
    def explain(self) -> DotMap:
        """Return the current stored value of ILM Explain"""
        return self._explain

    @property
    def next_phase(self) -> str:
        """Return the next phase in the index's ILM journey"""
        retval = None
        if self._explain.phase == 'delete':
            self.logger.warning('Already on "delete" phase. No more phases to advance')
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

    @property
    def policy_phases(self) -> t.Sequence[str]:
        """Return a list of phases in the ILM policy"""
        return list(self._phases.keys())

    def advance(
        self,
        phase: t.Union[str, None] = None,
        action: t.Union[str, None] = None,
        name: t.Union[str, None] = None,
    ) -> None:
        """Advance index to next ILM phase"""

        def wait(phase: str) -> None:
            """Wait for the phase change"""
            counter = 0
            sleep(1.5)  # Initial wait since we set ILM to poll every second
            while self._explain.phase != phase:
                sleep(PAUSE_VALUE)
                self.update()
                counter += 1
                self.count_logging(counter)

        if self._explain.phase == 'delete':
            self.logger.warning('Already on "delete" phase. No more phases to advance')
        else:
            self.logger.debug('current_step: %s', self.current_step)
            next_step = self.next_step(phase, action=action, name=name)
            self.logger.debug('next_step: %s', next_step)
            if next_step:
                ilm_move(self.client, self.name, self.current_step, next_step)
                wait(phase)
                self.logger.info('Index %s now on phase %s', self.name, phase)
            else:
                self.logger.error('next_step is a None value')
                self.logger.error('current_step: %s', self.current_step)

    def count_logging(self, counter: int) -> None:
        """Log messages based on how big counter is"""
        # Send a message every 10 loops
        if counter % 40 == 0:
            self.logger.info('Still working... Explain: %s', self._explain.asdict)
        if counter == 480:
            msg = 'Taking too long! Giving up on waiting'
            self.logger.critical(msg)
            raise ResultNotExpected(msg)

    def get_explain_data(self) -> t.Dict:
        """Get the ILM explain data and return it"""
        try:
            return ilm_explain(self.client, self.name)
        except NameChanged as err:
            self.logger.debug('Passing along upstream exception...')
            raise NameChanged from err
        except ResultNotExpected as err:
            msg = f'Unable to get ilm_explain API call results. Error: {err}'
            self.logger.critical(msg)
            raise ResultNotExpected(msg) from err

    def next_step(
        self,
        phase: t.Union[str, None] = None,
        action: t.Union[str, None] = None,
        name: t.Union[str, None] = None,
    ) -> t.Dict:
        """Determine the next ILM step based on the current phase, action, and name"""
        err1 = bool((action is not None) and (name is None))
        err2 = bool((action is None) and (name is not None))
        if err1 or err2:
            msg = 'If either action or name is specified, both must be'
            self.logger.critical(msg)
            raise TestbedMisconfig(msg)
        if not phase:
            phase = self.next_phase
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
            self.logger.critical(msg)
            raise ResultNotExpected(msg)
        if len(res['indices']) > 1:
            msg = f'{name} resolved to multiple indices: {res["indices"]}'
            self.logger.critical(msg)
            raise ResultNotExpected(msg)
        return res['indices'][0]['name']

    def update(self) -> None:
        """Update self._explain with the latest from :py:meth:`get_explain_data`"""
        try:
            self._explain = DotMap(self.get_explain_data())
        except NameChanged as err:
            self.logger.debug('Passing along upstream exception...')
            raise NameChanged from err

    def wait4complete(self) -> None:
        """Wait for the ILM phase change to complete both the action and step"""
        counter = 0
        self.logger.debug('Waiting for current action and step to complete')
        self.logger.debug(
            'Action: %s --- Step: %s', self._explain.action, self._explain.step
        )
        while not bool(
            self._explain.action == 'complete' and self._explain.step == 'complete'
        ):
            counter += 1
            sleep(PAUSE_VALUE)
            if counter % 10 == 0:
                self.logger.debug(
                    'Action: %s --- Step: %s', self._explain.action, self._explain.step
                )
            try:
                self.count_logging(counter)
            except ResultNotExpected as err:
                self.logger.critical(
                    'Breaking the loop. Explain: %s', self._explain.toDict()
                )
                raise ResultNotExpected from err
            try:
                self.update()
            except NameChanged as err:
                self.logger.debug('Passing along upstream exception...')
                raise NameChanged from err
