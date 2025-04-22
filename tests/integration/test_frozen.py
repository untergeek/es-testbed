"""Frozen tier tests. Mixed ILM"""

from . import TestAny


class TestDataStream(TestAny):
    """TestDataStream"""

    scenario = "frozen_ds"


class TestIndices(TestAny):
    """TestIndices"""

    scenario = "frozen"


class TestRolloverIndices(TestAny):
    """TestRolloverIndices"""

    scenario = "frozen_rollover"


class TestRolloverIndicesILM(TestAny):
    """TestRolloverIndicesILM"""

    scenario = "frozen_ilm"
