"""Deprecated es_testbed.helpers.es_api module"""

# pylint: disable=W0401,W0614
import warnings
from ..es_api import *

warnings.warn(
    (
        "es_testbed.helpers.es_api is deprecated. Use es_testbed.es_api instead. "
        "Will be removed in 1.0.0"
    ),
    DeprecationWarning,
    stacklevel=2,
)
