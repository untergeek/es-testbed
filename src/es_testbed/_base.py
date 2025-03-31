"""Base TestBed Class"""

import typing as t
import logging
from importlib import import_module
from datetime import datetime, timezone
from shutil import rmtree
import tiered_debug as debug
from es_testbed.exceptions import ResultNotExpected
from es_testbed.defaults import NAMEMAPPER
from es_testbed.helpers.es_api import delete, get
from es_testbed.helpers.utils import prettystr, process_preset
from es_testbed._plan import PlanBuilder
from es_testbed.mgrs import (
    ComponentMgr,
    DataStreamMgr,
    IlmMgr,
    IndexMgr,
    SnapshotMgr,
    TemplateMgr,
)

if t.TYPE_CHECKING:
    from elasticsearch8 import Elasticsearch

logger = logging.getLogger('es_testbed.TestBed')

# pylint: disable=R0902,R0913,R0917

# Preset Import
# This imports the preset directory which must include the following files:
# - A plan YAML file.
# - A buildlist YAML file.
# - A functions.py file (the actual python code), which must contain a
#   function named doc_generator(). This function must accept all kwargs from
#   the buildlist's options
# - A definitions.py file, which is a Python variable file that helps find
#   the path to the module, etc., as well as import the plan, the buildlist,
#   the mappings and settings, etc. This must at least include a get_plan()
#   function that returns a dictionary of a plan.
# - A mappings.json file (contains the index mappings your docs need)
# - A settings.json file (contains the index settings)
#
# Any other files can be included to help your doc_generator function, e.g.
# Faker definitions and classes, etc. Once the preset module is imported,
# relative imports should work.


class TestBed:
    """TestBed Class"""

    __test__ = False  # Without this, this appears to be test class because of the name

    def __init__(
        self,
        client: t.Optional['Elasticsearch'] = None,
        builtin: t.Optional[str] = None,
        path: t.Optional[str] = None,
        ref: t.Optional[str] = None,
        url: t.Optional[str] = None,
        scenario: t.Optional[str] = None,
    ):
        debug.lv2('Initializing TestBed object...')
        #: The plan settings
        self.settings = None

        modpath, tmpdir = process_preset(builtin, path, ref, url)
        if modpath is None:
            msg = 'Must define a preset'
            logger.critical(msg)
            raise ValueError(msg)

        try:
            debug.lv4('TRY: Attempting to import preset module')
            debug.lv5(f'preset module: {modpath}')
            preset = import_module(f'{modpath}.definitions')
            self.settings = preset.get_plan(scenario)
        except ImportError as err:
            logger.critical('Preset settings incomplete or incorrect')
            raise err
        debug.lv5(f'Preset module imported: {modpath}')

        self.settings['modpath'] = modpath
        if scenario:
            debug.lv5(f'Using scenario: {scenario}')
            self.settings['scenario'] = scenario
        if tmpdir:
            debug.lv5(f'Using tmpdir: {tmpdir}')
            self.settings['tmpdir'] = tmpdir

        #: The Elasticsearch client object
        self.client = client
        #: The test plan
        self.plan = None

        # Set up for tracking
        #: The ILM entity manager
        self.ilmmgr = None
        #: The Component Template entity manager
        self.componentmgr = None
        #: The (index) Template entity manager
        self.templatemgr = None
        #: The Snapshot entity manager
        self.snapshotmgr = None
        #: The Index entity manager
        self.indexmgr = None
        #: The data_stream entity manager
        self.data_streammgr = None

    def _erase(self, kind: str, lst: t.Sequence[str]) -> None:
        debug.lv2('Starting method...')
        overall_success = True
        if not lst:
            debug.lv3(f'{kind}: nothing to delete.')
            return True
        if kind == 'ilm':  # ILM policies can't be batch deleted
            ilm = [self._while(kind, x) for x in lst]
            overall_success = False not in ilm  # No False values == True
        else:
            overall_success = self._while(kind, ','.join(lst))
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {overall_success}')
        return overall_success

    def _fodder_generator(
        self,
    ) -> t.Generator[str, t.Sequence[str], None]:
        """Method to delete everything matching our pattern(s)"""
        debug.lv2('Starting method...')
        items = ['index', 'data_stream', 'snapshot', 'template', 'component', 'ilm']
        for i in items:
            if i == 'snapshot' and self.plan.repository is None:
                debug.lv4('No repository, no snapshots.')
                continue
            pattern = f'*{self.plan.prefix}-{NAMEMAPPER[i]}-{self.plan.uniq}*'
            entities = get(self.client, i, pattern, repository=self.plan.repository)
            yield (i, entities)
        debug.lv3('Exiting method')

    def _while(self, kind: str, item: str) -> bool:
        debug.lv2('Starting method...')
        count = 1
        success = False
        exc = None
        while count < 4 and not success:
            try:
                debug.lv4(f'TRY: Deleting {kind} "{item}"')
                success = delete(
                    self.client, kind, item, repository=self.plan.repository
                )
                break
            except ResultNotExpected as err:
                debug.lv1(f'Tried deleting "{item}" {count} time(s)')
                exc = err
            count += 1
        if not success:
            logger.warning(
                f'Failed to delete "{item}" after {count - 1} tries. '
                f'Final error: {exc}'
            )
        return success

    def get_ilm_polling(self) -> None:
        """
        Get current ILM polling settings and store them in self.plan.polling_interval
        """
        debug.lv2('Starting method...')
        debug.lv3('Storing current ILM polling settings, if any...')
        try:
            debug.lv4('TRY: Getting cluster settings')
            res = dict(self.client.cluster.get_settings())
            debug.lv5(f'Cluster settings: {prettystr(res)}')
        except Exception as err:
            logger.critical('Unable to get persistent cluster settings')
            logger.critical('This could be permissions, or something larger.')
            logger.critical(f'Exception: {prettystr(err)}')
            logger.critical('Exiting.')
            raise err
        try:
            debug.lv4('TRY: Getting ILM Polling Interval from settings')
            retval = res['persistent']['indices']['lifecycle']['poll_interval']
        except KeyError:
            debug.lv3('No setting for indices.lifecycle.poll_interval. Must be default')
            retval = None  # Must be an actual value to go into a DotMap
        if retval == '1s':
            msg = (
                'ILM polling already set at 1s. A previous run most likely did not '
                'tear down properly. Resetting to null after this run'
            )
            logger.warning(msg)
            retval = None  # Must be an actual value to go into a DotMap
        self.plan.ilm_polling_interval = retval
        debug.lv3(f'Stored ILM Polling Interval: {retval}')
        debug.lv3('Exiting method')

    def ilm_polling(self, interval: t.Union[str, None] = None) -> t.Dict:
        """Return persistent cluster settings to speed up ILM polling during testing"""
        debug.lv2('Starting method...')
        retval = {'indices.lifecycle.poll_interval': interval}
        debug.lv3('Exiting method, returning value')
        debug.lv5(f'Value = {retval}')
        return retval

    def set_debug_tier(self, tier: int) -> None:
        """
        Set the debug tier globally for this module
        """
        debug.set_level(tier)

    def setup(self) -> None:
        """Setup the instance"""
        debug.lv2('Starting method...')
        start = datetime.now(timezone.utc)
        # If we build self.plan here, then we can modify settings before setup()
        self.plan = PlanBuilder(settings=self.settings).plan
        self.get_ilm_polling()
        debug.lv5(f'Setting: {self.ilm_polling(interval="1s")}')
        self.client.cluster.put_settings(persistent=self.ilm_polling(interval='1s'))
        self.setup_entitymgrs()
        end = datetime.now(timezone.utc)
        debug.lv1(f'Testbed setup elapsed time: {(end - start).total_seconds()}')
        debug.lv3('Exiting method')

    def setup_entitymgrs(self) -> None:
        """
        Setup each EntityMgr child class
        """
        debug.lv2('Starting method...')
        kw = {'client': self.client, 'plan': self.plan}

        self.ilmmgr = IlmMgr(**kw)
        self.ilmmgr.setup()
        self.componentmgr = ComponentMgr(**kw)
        self.componentmgr.setup()
        self.templatemgr = TemplateMgr(**kw)
        self.templatemgr.setup()
        self.snapshotmgr = SnapshotMgr(**kw)
        self.snapshotmgr.setup()
        if self.plan.type == 'indices':
            self.indexmgr = IndexMgr(**kw, snapmgr=self.snapshotmgr)
            self.indexmgr.setup()
        if self.plan.type == 'data_stream':
            self.data_streammgr = DataStreamMgr(**kw, snapmgr=self.snapshotmgr)
            self.data_streammgr.setup()
        debug.lv3('Exiting method')

    def teardown(self) -> None:
        """Tear down anything we created"""
        debug.lv2('Starting method...')
        start = datetime.now(timezone.utc)
        successful = True
        if self.plan.tmpdir:
            debug.lv3(f'Removing tmpdir: {self.plan.tmpdir}')
            rmtree(self.plan.tmpdir)  # Remove the tmpdir stored here
        for kind, list_of_kind in self._fodder_generator():
            if not self._erase(kind, list_of_kind):
                successful = False
        persist = self.ilm_polling(interval=self.plan.ilm_polling_interval)
        debug.lv3(
            f'Restoring ILM polling to previous value: '
            f'{self.plan.ilm_polling_interval}'
        )
        self.client.cluster.put_settings(persistent=persist)
        end = datetime.now(timezone.utc)
        debug.lv1(f'Testbed teardown elapsed time: {(end - start).total_seconds()}')
        if successful:
            logger.info('Cleanup successful')
        else:
            logger.error('Cleanup was unsuccessful/incomplete')
        self.plan.cleanup = successful
        debug.lv3('Exiting method')
