"""es-testbed Exceptions"""
### Parent exception
class TestbedException(Exception):
    """
    Base class for all exceptions raised by the tool which are not Elasticsearch
    or es_client exceptions.
    """
###
class TestbedMisconfig(TestbedException):
    """
    There was a misconfiguration encountered.
    """
