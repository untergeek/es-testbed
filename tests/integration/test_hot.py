"""Hot tier tests. No ILM. No searchable snapshots."""

from . import TestAny


class TestDataStream(TestAny):
    """TestDataStream"""

    scenario = 'hot_ds'


class TestIndices(TestAny):
    """TestIndices"""

    scenario = 'hot'


class TestRolloverIndices(TestAny):
    """TestRolloverIndices"""

    scenario = 'hot_rollover'
