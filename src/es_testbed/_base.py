"""Base TestBed Class"""

import typing as t
from datetime import datetime, timezone
from dotmap import DotMap
from .exceptions import ResultNotExpected
from .defaults import NAMEMAPPER
from .helpers.es_api import delete, get
from .helpers.utils import getlogger
from ._plan import PlanBuilder
from .mgrs import (
    ComponentMgr,
    DataStreamMgr,
    IlmMgr,
    IndexMgr,
    SnapshotMgr,
    TemplateMgr,
)

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

# pylint: disable=R0902,W0719


class TestBed:
    """TestBed Class"""

    __test__ = False

    def __init__(
        self,
        client: 'Elasticsearch' = None,
        plan: DotMap = None,
        autobuild: t.Optional[bool] = False,
    ):
        self.logger = getlogger('es_testbed.TestBed')
        self.client = client
        if plan is None:
            plan = PlanBuilder().plan  # Use defaults
        self.plan = plan

        # Set up for tracking
        self.ilmmgr = None
        self.componentmgr = None
        self.templatemgr = None
        self.snapshotmgr = None
        self.indexmgr = None
        self.data_streammgr = None

        if autobuild:
            self.setup()

    def _erase(self, kind: str, lst: t.Sequence[str]) -> None:
        overall_success = True
        if not lst:
            self.logger.debug('%s: nothing to delete.', kind)
            return True
        if kind == 'ilm':  # ILM policies can't be batch deleted
            ilm = [self._while(kind, x) for x in lst]
            overall_success = False not in ilm  # No False values == True
        else:
            overall_success = self._while(kind, ','.join(lst))
        return overall_success

    def _fodder_generator(
        self,
    ) -> t.Generator[str, t.Sequence[str], None]:
        """Method to delete everything matching our pattern(s)"""
        items = ['index', 'data_stream', 'snapshot', 'template', 'component', 'ilm']
        for i in items:
            if i == 'snapshot' and self.plan.repository is None:
                self.logger.debug('No repository, no snapshots.')
                continue
            # self.logger.debug('Generating a list of type "%s"', i)
            pattern = f'*{self.plan.prefix}-{NAMEMAPPER[i]}-{self.plan.uniq}*'
            entities = get(self.client, i, pattern, repository=self.plan.repository)
            yield (i, entities)

    def _while(self, kind: str, item: str) -> bool:
        count = 1
        success = False
        exc = None
        while count < 4 and not success:
            try:
                success = delete(
                    self.client, kind, item, repository=self.plan.repository
                )
                break
            except ResultNotExpected as err:
                self.logger.debug('Tried deleting "%s" %s time(s)', item, count)
                exc = err
            count += 1
        if not success:
            self.logger.warning(
                'Failed to delete "%s" after %s tries. Final error: %s',
                item,
                count - 1,
                exc,
            )
        return success

    def get_ilm_polling(self) -> None:
        """
        Get current ILM polling settings and store them in self.plan.polling_interval
        """
        self.logger.info('Storing current ILM polling settings, if any...')
        try:
            res = self.client.cluster.get_settings()
            self.logger.debug('Cluster settings: %s', res)
        except Exception as err:
            self.logger.critical('Unable to get persistent cluster settings')
            self.logger.critical('This could be permissions, or something larger.')
            self.logger.critical('Exception: %s', err)
            self.logger.critical('Exiting.')
            raise Exception from err
        try:
            retval = res['persistent']['indices']['lifecycle']['poll_interval']
        except KeyError:
            self.logger.debug(
                'No setting for indices.lifecycle.poll_interval. Must be default'
            )
            retval = None  # Must be an actual value to go into a DotMap
        if retval == '1s':
            msg = (
                'ILM polling already set at 1s. A previous run most likely did not '
                'tear down properly. Resetting to null after this run'
            )
            self.logger.warning(msg)
            retval = None  # Must be an actual value to go into a DotMap
        self.logger.debug('retval = %s', retval)
        self.plan.ilm_polling_interval = retval
        self.logger.info('Stored ILM Polling Interval: %s', retval)

    def ilm_polling(self, interval: t.Union[str, None] = None) -> t.Dict:
        """Return persistent cluster settings to speed up ILM polling during testing"""
        return {'indices.lifecycle.poll_interval': interval}

    def setup(self) -> None:
        """Setup the instance"""
        start = datetime.now(timezone.utc)
        self.get_ilm_polling()
        self.logger.info('Setting: %s', self.ilm_polling(interval='1s'))
        self.client.cluster.put_settings(persistent=self.ilm_polling(interval='1s'))
        self.setup_entitymgrs()
        end = datetime.now(timezone.utc)
        self.logger.info(
            'Testbed setup elapsed time: %s', (end - start).total_seconds()
        )

    def setup_entitymgrs(self) -> None:
        """
        Setup each EntityMgr child class
        """
        kw = {'client': self.client, 'plan': self.plan}
        self.ilmmgr = IlmMgr(**kw)
        self.componentmgr = ComponentMgr(**kw)
        self.templatemgr = TemplateMgr(**kw)
        self.snapshotmgr = SnapshotMgr(**kw)
        if self.plan.type == 'indices':
            self.indexmgr = IndexMgr(**kw, snapmgr=self.snapshotmgr)
        if self.plan.type == 'data_stream':
            self.data_streammgr = DataStreamMgr(**kw, snapmgr=self.snapshotmgr)

    def teardown(self) -> None:
        """Tear down anything we created"""
        start = datetime.now(timezone.utc)
        successful = True
        for kind, list_of_kind in self._fodder_generator():
            if not self._erase(kind, list_of_kind):
                successful = False
        persist = self.ilm_polling(interval=self.plan.ilm_polling_interval)
        self.logger.info(
            'Restoring ILM polling to previous value: %s',
            self.plan.ilm_polling_interval,
        )
        self.client.cluster.put_settings(persistent=persist)
        end = datetime.now(timezone.utc)
        self.logger.info(
            'Testbed teardown elapsed time: %s', (end - start).total_seconds()
        )
        if successful:
            self.logger.info('Cleanup successful')
        else:
            self.logger.error('Cleanup was unsuccessful/incomplete')
        self.plan.cleanup = successful
