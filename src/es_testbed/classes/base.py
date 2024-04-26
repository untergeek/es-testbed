"""Base TestBed Class"""

import typing as t
from datetime import datetime, timezone
from dotmap import DotMap
from elasticsearch8 import Elasticsearch
from es_testbed.helpers.es_api import delete
from es_testbed.helpers.utils import getlogger
from .plan import PlanBuilder
from .tracker import Tracker

# pylint: disable=broad-exception-caught


class TestBed:
    """Base TestBed Class"""

    __test__ = False

    def __init__(self, client: Elasticsearch = None, plan: DotMap = None, autobuild: t.Optional[bool] = False):
        """Initialize"""
        self.logger = getlogger('es_testbed.TestBed')
        self.client = client
        if plan is None:
            plan = PlanBuilder().plan  # Use defaults
        self.plan = plan
        self.tracker = Tracker(client=client, plan=plan, autobuild=autobuild)

    def failsafe_teardown(self):
        """Fallback method to delete things still remaining"""
        items = ['index', 'data_stream', 'snapshot', 'template', 'component', 'ilm']
        for i in items:
            if i == 'snapshot':
                snaps = ','.join(self.plan.failsafes[i])
                self.client.snapshot.delete(snapshot=snaps, repository=self.plan.repository)
            else:
                self._loop_teardown(i, self.plan.failsafes[i])

    def ilm_polling(self, interval: t.Union[str, None] = None):
        """Return persistent cluster settings to speed up ILM polling during testing"""
        return {'indices.lifecycle.poll_interval': interval}

    def _loop_teardown(self, kind: str, lst: t.Sequence[str]) -> None:
        for item in lst:
            try:
                delete(self.client, kind, item)
            except Exception as err:
                self.logger.error('Tried deleting %s via %s. Error: %s', kind, item, err)

    def setup(self):
        """Setup the instance"""
        start = datetime.now(timezone.utc)
        self.logger.info('Setting: %s', self.ilm_polling(interval='1s'))
        self.client.cluster.put_settings(persistent=self.ilm_polling(interval='1s'))
        self.tracker.setup()
        end = datetime.now(timezone.utc)
        self.logger.info('Testbed setup elapsed time: %s', (end - start).total_seconds())

    def teardown(self):
        """Tear down anything we created"""
        start = datetime.now(timezone.utc)
        try:
            self.tracker.teardown()
        except Exception as err:
            self.logger.error('An exception occurred during teardown: %s', err)
            self.logger.info('Remaining entities: %s', self.plan)
            self.logger.info('Attempting failsafe cleanup...')
            self.failsafe_teardown()
        # Do we clean up the failsafes in the plan?
        self.logger.info('Restoring ILM polling to default: %s', self.ilm_polling(interval=None))
        self.client.cluster.put_settings(persistent=self.ilm_polling(interval=None))
        end = datetime.now(timezone.utc)
        self.logger.info('Testbed teardown elapsed time: %s', (end - start).total_seconds())
