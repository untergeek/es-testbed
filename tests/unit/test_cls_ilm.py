"""Test functions in es_testbed.defaults"""
# pylint: disable=protected-access, import-error
# add import-error here ^^^ to avoid false-positives for the local import
from unittest import TestCase
from es_testbed.defaults import TESTPLAN
from es_testbed.classes.ilm import IlmBuilder

class TestDefaultFunctions(TestCase):
    """Ensure test coverage of simple functions that might be deprecated in the future"""
    def test_cls_ilm_defaults(self):
        """Ensure matching output"""
        defaults = TESTPLAN['ilm']
        ilm = IlmBuilder()
        self.assertEqual(ilm.tiers, defaults['tiers'])
        self.assertEqual(ilm.forcemerge, defaults['forcemerge'])
        self.assertEqual(ilm.max_num_segments, defaults['max_num_segments'])
        self.assertEqual(ilm.repository, defaults['repository'])
    def test_cls_ilm_defaults_policy(self):
        """Ensure matching output"""
        expected = {'phases': {'delete': {'actions': {'delete': {}}, 'min_age': '3h'},
                        'hot': {'actions': {'rollover': {'max_age': '10m',
                        'max_primary_shard_size': '1gb'}}}}}
        ilm = IlmBuilder()
        self.assertEqual(ilm.policy, expected)
    def test_cls_ilm_forcemerge(self):
        """Ensure matching output"""
        expected = {'phases': {'delete': {'actions': {'delete': {}}, 'min_age': '3h'},
                        'hot': {'actions': {'rollover': {'max_age': '10m',
                        'max_primary_shard_size': '1gb'}, 'forcemerge': {'max_num_segments': 1}}}}}
        ilm = IlmBuilder()
        ilm.forcemerge = True
        self.assertEqual(ilm.policy, expected)
    def test_cls_ilm_mns(self):
        """Ensure matching output"""
        expected = {'phases': {'delete': {'actions': {'delete': {}}, 'min_age': '3h'},
                        'hot': {'actions': {'rollover': {'max_age': '10m',
                        'max_primary_shard_size': '1gb'}, 'forcemerge': {'max_num_segments': 2}}}}}
        ilm = IlmBuilder()
        ilm.forcemerge = True
        ilm.max_num_segments = 2
        self.assertEqual(ilm.policy, expected)
    def test_cls_ilm_cold(self):
        """Ensure matching output"""
        expected = {'phases': {
            'hot': {'actions': {'rollover': {'max_age': '10m', 'max_primary_shard_size': '1gb'}}},
            'cold': {'actions': {'searchable_snapshot': {'snapshot_repository': 'repo'}}, 'min_age': '1h'},
        }}
        ilm = IlmBuilder()
        ilm.tiers = ['hot', 'cold']
        ilm.repository = 'repo'
        self.assertEqual(ilm.policy, expected)
