"""Entity Class Definition"""

import typing as t
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.defaults import NAMEMAPPER, PLURALMAP
from es_testbed.helpers.es_api import delete, get
from es_testbed.helpers.utils import getlogger, uniq_values

# pylint: disable=missing-docstring,broad-exception-caught,too-many-instance-attributes


class EntityMgr:
    kind = 'entity_type'
    listname = 'entity_mgrs'

    def __init__(
        self, client: t.Union[Elasticsearch, None] = None, plan: DotMap = None, autobuild: t.Optional[bool] = True
    ):
        self.logger = getlogger('es_testbed.EntityMgr')
        self.client = client
        self.plan = plan
        self.success = False
        if autobuild:
            self.setup()

    @property
    def entity_list(self):
        return self.plan[self.listname]

    @entity_list.setter
    def entity_list(self, value: t.Sequence) -> None:
        self.plan[self.listname] = value

    @property
    def entity_root(self) -> str:
        return f'{self.plan.prefix}-{self.ident()}-{self.plan.uniq}'

    @property
    def failsafe(self):
        return self.plan.failsafes[self.kind]

    @failsafe.setter
    def failsafe(self, value: t.Sequence) -> None:
        self.plan.failsafes[self.kind] = value

    @property
    def indexlist(self) -> t.Sequence[str]:
        return []  # Empty attribute/property waiting to be overridden

    @property
    def last(self) -> str:
        return self.entity_list[-1]  # Return the most recently appended item

    @property
    def logdisplay(self) -> str:
        return self.kind

    @property
    def name(self) -> str:
        return f'{self.entity_root}{self.suffix}'

    @property
    def pattern(self) -> str:
        return f'*{self.entity_root}*'

    @property
    def suffix(self) -> str:
        return f'-{len(self.entity_list) + 1:06}'

    def appender(self, name: str) -> None:
        self.failsafe.append(name)
        self.entity_list.append(name)

    def ident(self, dkey=None):
        if not dkey:
            dkey = self.kind
        return NAMEMAPPER[dkey]

    def iterate_clean(self) -> bool:
        succeed = True
        positions = []
        for idx, entity in enumerate(self.entity_list):  # There should only be one, but we cover it
            value = entity
            if self.kind == 'index':
                value = entity.name
            self.logger.debug('Deleting %s %s', self.logdisplay, value)
            try:
                delete(self.client, self.kind, value)
            except Exception as err:
                succeed = False
                msg = f'Unable to delete {self.logdisplay}: {value}. Error: {err}'
                self.logger.error(msg)
                continue
            self.logger.debug('Deleted %s %s', self.logdisplay, value)
            positions.append(idx)
        positions.sort()  # Sort first to ensure lowest to highest order
        for idx in reversed(positions):  # Reverse the list and iterate
            del self.entity_list[idx]  # Delete the value at position idx
            del self.plan.failsafes[self.kind][idx]  # Delete the value at position idx
        return succeed

    def scan(self) -> t.Sequence[str]:
        """Find all entities matching our pattern"""
        entities = get(self.client, self.kind, self.pattern)
        msg = f'{self.kind} entities found matching pattern "{self.pattern}": {entities}'
        self.logger.debug(msg)
        self.failsafe = entities
        return entities

    def setup(self):
        pass

    def teardown(self):
        display = PLURALMAP[self.kind] if self.kind in PLURALMAP else self.kind
        if not self.success:
            msg = f'Setup did not complete successfully. ' f'Manual cleanup of {display}s may be necessary.'
            self.logger.warning(msg)
        self.verify(correct=True)  # Catch any entities that might exist but not be in entity_list
        if self.entity_list:
            if self.iterate_clean():
                self.logger.info('Cleanup of %ss completed successfully.', display)

    def track_index(self, name: str) -> None:
        pass

    def verify(self, correct: bool = False) -> t.Union[t.Sequence[str], None]:
        retval = None
        diffs = False
        curr = self.scan()  # This is what entity_list _should_ look like.
        if self.kind == 'index':
            entities = self.indexlist
        else:
            entities = self.entity_list
        self.logger.debug('Getting unique values from scan output (not in self.entity_list)')
        scan_diff = uniq_values(entities, curr)
        self.logger.debug('Getting unique values in self.entity_list (not in scan)')
        entity_diff = uniq_values(curr, entities)
        if entity_diff:
            diffs = True
            self.logger.warning('Values in entity_list not found in scan: %s', entity_diff)
        if scan_diff:
            diffs = True
            self.logger.info('Values in scan not found in entity_list: %s', scan_diff)
        if diffs:
            if correct:
                self.logger.info('Correcting entity_list with values from scan: %s', curr)
                if self.kind == 'index':
                    self.entity_list = []
                    for index in curr:
                        self.track_index(index)  # We have to re-create the tracked entities
                else:
                    self.entity_list = curr
            else:
                self.logger.warning('Not correcting entity_list! Values should be: %s', curr)
                retval = curr
        return retval
