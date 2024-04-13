"""ILM Defining Class"""
from es_testbed.defaults import TESTPLAN
from es_testbed.helpers.utils import build_ilm_policy

# pylint: disable=missing-docstring

class IlmBuilder:
    """Define elements of an ILM policy"""
    def __init__(self):
        self.set_defaults()

    @property
    def tiers(self) -> list:
        return self._tiers
    @tiers.setter
    def tiers(self, value: list):
        self._tiers = value

    @property
    def forcemerge(self) -> bool:
        return self._forcemerge
    @forcemerge.setter
    def forcemerge(self, value: bool):
        self._forcemerge = value

    @property
    def max_num_segments(self) -> int:
        return self._max_num_segments
    @max_num_segments.setter
    def max_num_segments(self, value: int):
        self._max_num_segments = value

    @property
    def repository(self) -> str:
        return self._repository
    @repository.setter
    def repository(self, value: str):
        self._repository = value

    @property
    def policy(self) -> dict:
        self._policy = build_ilm_policy(
                tiers=self.tiers,
                fmerge=self.forcemerge,
                mns=self.max_num_segments,
                repo=self.repository
            )
        return self._policy
    @policy.setter
    def policy(self, value: dict):
        self._policy = value

    def set_defaults(self):
        defaults = TESTPLAN['ilm']
        self.tiers = defaults['tiers']
        self.forcemerge = defaults['forcemerge']
        self.max_num_segments = defaults['max_num_segments']
        self.repository = defaults['repository']
