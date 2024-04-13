"""Test functions in es_testbed.defaults"""
# pylint: disable=protected-access, import-error
# add import-error here ^^^ to avoid false-positives for the local import
from unittest import TestCase
from es_testbed.defaults import NAMEMAPPER
from es_testbed.classes.tracker import Tracker

class TestTracker(TestCase):
    """Ensure test coverage of simple functions that might be deprecated in the future"""
    def test_cls_tracker_namemgr_defaults(self):
        """Ensure matching output"""
        tracker = Tracker()
        namemgr = tracker.NameMgr()
        expected = 2
        assert expected == len(namemgr.patterns)
    def test_cls_tracker_namemgr_pfx(self):
        """Ensure matching output"""
        tracker = Tracker()
        namemgr = tracker.NameMgr()
        kind = 'ilm'
        expected = NAMEMAPPER[kind]
        assert expected == namemgr.pfx(kind)
    def test_cls_tracker_namemgr_match(self):
        """Ensure matching output"""
        pfx = 'test'
        kind = 'index'
        sfx = '000001'
        tracker = Tracker()
        namemgr = tracker.NameMgr(prefix=pfx)
        rnd = namemgr.rand
        expected = f"{pfx}-{NAMEMAPPER[kind]}-{rnd}-{sfx}"
        assert expected == namemgr.index
    def test_cls_tracker_namemgr_second(self):
        """Ensure matching output"""
        pfx = 'test'
        kind = 'index'
        sfx = '000002'
        tracker = Tracker()
        namemgr = tracker.NameMgr(prefix=pfx)
        rnd = namemgr.rand
        expected = f"{pfx}-{NAMEMAPPER[kind]}-{rnd}-{sfx}"
        tracker.indices.append(namemgr.index) # After this call, the list grows
        # so the call in the assert line will have end in 2 (see sfx)
        assert expected == namemgr.index
