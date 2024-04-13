"""Base TestBed Class"""

from es_testbed.classes.testplan import TestPlan
from es_testbed.classes.tracker import Tracker
from es_testbed.exceptions import TestbedMisconfig
from es_testbed.helpers import es_api

class TestBed:
    """Base TestBed Class"""
    def __init__(self, client, plan: TestPlan=None):
        """Initialize"""
        self.client = client
        self.plan = plan
        self.tracker = Tracker(plan)
        self.namemgr = self.tracker.namemgr

    def setup(self):
        """Setup the instance"""
        self.tracker.setup()

    def teardown(self):
        """Tear down anything we created"""
        self.tracker.teardown()
