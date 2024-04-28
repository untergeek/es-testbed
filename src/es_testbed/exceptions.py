"""es-testbed Exceptions"""


class TestbedException(Exception):  # parent exception
    """
    Base class for all exceptions raised by the tool which are not Elasticsearch
    or es_client exceptions.
    """

    __test__ = False


class MissingArgument(TestbedException):
    """
    An expected argument was missing
    """


class NameChanged(TestbedException):
    """
    An index name changed, likely due to an ILM promotion to cold or frozen
    """


class ResultNotExpected(TestbedException):
    """
    The result we got is not what we expected
    """


class TestbedFailure(TestbedException):
    """
    Whatever we were trying to do failed.
    """

    __test__ = False


class TestbedMisconfig(TestbedException):
    """
    There was a misconfiguration encountered.
    """

    __test__ = False


class TestPlanMisconfig(TestbedMisconfig):
    """
    There was a misconfiguration in a TestPlan.
    """

    __test__ = False


class TimeoutException(TestbedException):
    """
    An process took too long to complete
    """
