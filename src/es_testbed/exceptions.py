"""es-testbed Exceptions"""

from typing import Any, Tuple


class TestbedException(Exception):  # parent exception
    """
    Base class for all exceptions raised by the tool which are not Elasticsearch
    or es_client exceptions.

    For the 'errors' attribute, errors are ordered from
    most recently raised (index=0) to least recently raised (index=N)
    """

    __test__ = False

    def __init__(self, message: Any, errors: Tuple[Exception, ...] = ()):
        super().__init__(message)
        self.message = message
        self.errors = tuple(errors)

    def __repr__(self) -> str:
        parts = [repr(self.message)]
        if self.errors:
            parts.append(f"errors={self.errors!r}")
        return f'{self.__class__.__name__}({", ".join(parts)})'

    def __str__(self) -> str:
        return str(self.message)


class MissingArgument(TestbedException):
    """
    An expected argument was missing
    """


class NameChanged(TestbedException):
    """
    An index name changed, likely due to an ILM promotion to cold or frozen
    """


class StepChanged(TestbedException):
    """
    The current step changed since the initial API call was formed
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
