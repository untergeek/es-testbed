"""Test functions in es_testbed.helpers.utils"""
# pylint: disable=protected-access, import-error
# add import-error here ^^^ to avoid false-positives for the local import
from unittest import TestCase
from es_testbed.helpers import utils

class TestUtils(TestCase):
    """Ensure test coverage of simple functions that might be deprecated in the future"""
    def test_build_ilm_phase_warm(self):
        """Ensure matching output"""
        tier = 'warm'
        expected = {tier: {'min_age': '2d', 'actions': {}}}
        self.assertEqual(utils.build_ilm_phase(tier), expected)
    def test_build_ilm_phase_cold(self):
        """Ensure matching output"""
        tier = 'cold'
        repo = 'repo'
        expected = {tier: {'min_age': '3d', 'actions': 
                           {'searchable_snapshot': {'snapshot_repository': 'repo'}}}}
        self.assertEqual(utils.build_ilm_phase(tier, repository=repo), expected)
    def test_build_ilm_phase_delete(self):
        """Ensure matching output"""
        tier = 'delete'
        expected = {tier: {'min_age': '5d', 'actions': {'delete':{}}}}
        self.assertEqual(utils.build_ilm_phase(tier), expected)
    def test_build_ilm_policy_hot_delete(self):
        """Ensure matching output"""
        tiers = ['hot', 'delete']
        expected = {
            'phases': {
                'hot': {'actions': {'rollover': {
                    'max_age': '1d', 'max_primary_shard_size': '1gb'}}},
                'delete': {'actions': {'delete': {}}, 'min_age': '5d'}
            }
        }
        self.assertEqual(utils.build_ilm_policy(tiers), expected)
    def test_build_ilm_policy_hot_frozen_delete(self):
        """Ensure matching output"""
        tiers = ['hot', 'frozen', 'delete']
        repo = 'repo'
        expected = {
            'phases': {
                'hot': {
                    'actions': {'rollover': {'max_age': '1d', 'max_primary_shard_size': '1gb'}}
                },
                'frozen': {
                    'actions': {'searchable_snapshot': {'snapshot_repository': repo}},
                    'min_age': '4d'},
                'delete': {'actions': {'delete': {}}, 'min_age': '5d'}
            }
        }
        self.assertEqual(utils.build_ilm_policy(tiers, repository=repo), expected)
    def test_build_ilm_policy_hot_forcemerge_cold_delete(self):
        """Ensure matching output"""
        tiers = ['hot', 'cold', 'delete']
        repo = 'repo'
        mns = 3
        expected = {
            'phases': {
                'hot': {
                    'actions': {
                        'rollover': {'max_age': '1d', 'max_primary_shard_size': '1gb'},
                        'forcemerge': {'max_num_segments': mns},
                    }
                },
                'cold': {
                    'actions': {'searchable_snapshot': {'snapshot_repository': repo}},
                    'min_age': '3d'},
                'delete': {'actions': {'delete': {}}, 'min_age': '5d'}
            }
        }
        self.assertEqual(
            utils.build_ilm_policy(tiers, repository=repo, forcemerge=True, max_num_segments=mns),
            expected
        )
