"""Base TestBed Class"""
import typing as t
from datetime import datetime, timezone
from elasticsearch8 import Elasticsearch
from es_testbed.defaults import TESTPLAN
from es_testbed.helpers.utils import getlogger
from .testplan import TestPlan
from .tracker import Tracker

class TestBed:
    """Base TestBed Class"""
    def __init__(
            self,
            client: Elasticsearch = None,
            plan: TestPlan = None,
            autobuild: t.Optional[bool] = False,
        ):
        """Initialize"""
        self.logger = getlogger('es_testbed.TestBed')
        self.client = client
        if plan is None:
            plan = TESTPLAN
        self.tracker = Tracker(client=client, plan=plan, autobuild=autobuild)

    def ilm_polling(self, interval: t.Union[str, None] = None):
        """Return persistent cluster settings to speed up ILM polling during testing"""
        return {'indices.lifecycle.poll_interval': interval}

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
        self.tracker.teardown()
        self.logger.info('Restoring ILM polling to default: %s', self.ilm_polling(interval=None))
        self.client.cluster.put_settings(persistent=self.ilm_polling(interval=None))
        end = datetime.now(timezone.utc)
        self.logger.info('Testbed teardown elapsed time: %s', (end - start).total_seconds())
