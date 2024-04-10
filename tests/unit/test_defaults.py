"""Test functions in es_testbed.defaults"""
# pylint: disable=protected-access, import-error
# add import-error here ^^^ to avoid false-positives for the local import
from unittest import TestCase
from es_testbed.defaults import ilm_force_merge, ilm_phase


class TestDefaultFunctions(TestCase):
    """Ensure test coverage of simple functions that might be deprecated in the future"""
    def test_ilm_force_merge_default(self):
        """Ensure matching output"""
        expected = {'forcemerge': {'max_num_segments': 1}}
        self.assertEqual(ilm_force_merge(), expected)
    def test_ilm_force_merge_mns(self):
        """Ensure matching output"""
        mns = 2
        expected = {'forcemerge': {'max_num_segments': mns}}
        self.assertEqual(ilm_force_merge(max_num_segments=mns), expected)
    def test_ilm_phase_warm(self):
        """Ensure matching output"""
        tier = 'warm'
        expected = {tier: {'min_age': '30m', 'actions': {}}}
        self.assertEqual(ilm_phase(tier), expected)
    def test_ilm_phase_cold(self):
        """Ensure matching output"""
        tier = 'cold'
        expected = {tier: {'min_age': '1h', 'actions': {}}}
        self.assertEqual(ilm_phase(tier), expected)
