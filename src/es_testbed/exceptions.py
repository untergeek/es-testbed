"""es-testbed Exceptions"""
### Parent exception
class TestbedException(Exception):
    """
    Base class for all exceptions raised by the tool which are not Elasticsearch
    or es_client exceptions.
    """
###
class MissingArgument(TestbedException):
    """
    An expected argument was missing
    """

class ResultNotExpected(TestbedException):
    """
    The result we got is not what we expected
    """

class TestbedFailure(TestbedException):
    """
    Whatever we were trying to do failed.
    """

class TestbedMisconfig(TestbedException):
    """
    There was a misconfiguration encountered.
    """

class TimeoutException(TestbedException):
    """
    An process took too long to complete
    """
