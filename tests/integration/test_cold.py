"""Cold tier tests. Mixed ILM"""

from . import TestAny


class TestDataStream(TestAny):
    """TestDataStream"""

    scenario = "cold_ds"


class TestIndices(TestAny):
    """TestIndices"""

    scenario = "cold"


class TestRolloverIndices(TestAny):
    """TestRolloverIndices"""

    scenario = "cold_rollover"


class TestRolloverIndicesILM(TestAny):
    """TestRolloverIndicesILM"""

    scenario = "cold_ilm"
