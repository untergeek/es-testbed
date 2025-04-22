"""ES Testbed: A Python library for generating Elasticsearch test scenarios."""

from datetime import datetime
from es_testbed._base import TestBed
from es_testbed._plan import PlanBuilder
from es_testbed.debug import debug

__version__ = "0.11.3"

FIRST_YEAR = 2025
now = datetime.now()
if now.year == FIRST_YEAR:
    COPYRIGHT_YEARS = "2025"
else:
    COPYRIGHT_YEARS = f"2025-{now.year}"

__author__ = "Aaron Mildenstein"
__copyright__ = f"{COPYRIGHT_YEARS}, {__author__}"
__license__ = "Apache 2.0"
__status__ = "Development"
__description__ = (
    "Library to help with building and tearing down indices, data streams, "
    "repositories and snapshots, and other test scenarios in Elasticsearch."
)
__url__ = "https://github.com/untergeek/es-testbed"
__email__ = "aaron@mildensteins.com"
__maintainer__ = "Aaron Mildenstein"
__maintainer_email__ = f"{__email__}"
__keywords__ = [
    "elasticsearch",
    "index",
    "testing",
    "datastream",
    "repository",
    "snapshot",
]
__classifiers__ = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
]

__all__ = [
    "TestBed",
    "PlanBuilder",
    "debug",
    "__author__",
    "__copyright__",
    "__version__",
]
