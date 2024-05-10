"""Module that will generate docs for Searchable Snapshot Test"""

import typing as t
import random
import string
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def iso8601_now() -> str:
    """
    :returns: An ISO8601 timestamp based on now
    :rtype: str

    Because Python 3.12 now requires non-naive timezone declarations, we must change.

    The new way:

    .. code-block:: python

      datetime.now(timezone.utc).isoformat()

    Result: ``2024-04-16T16:00:00+00:00``

    Note that the +00:00 is appended now where we affirmatively declare the
    UTC timezone

    As a result, we will use this function to prune away the timezone if it is
    +00:00 and replace it with Z, which is shorter Zulu notation for UTC (which
    Elasticsearch uses)

    We are MANUALLY, FORCEFULLY declaring timezone.utc, so it should ALWAYS be
    +00:00, but could in theory sometime show up as a Z, so we test for that.
    """

    parts = datetime.now(timezone.utc).isoformat().split('+')
    if len(parts) == 1:
        if parts[0][-1] == 'Z':
            return parts[0]  # _______ It already ends with a Z for Zulu/UTC time
        return f'{parts[0]}Z'  # _____ It doesn't end with a Z so we put one there
    if parts[1] == '00:00':
        return f'{parts[0]}Z'  # _____ It doesn't end with a Z so we put one there
    return f'{parts[0]}+{parts[1]}'  # Fallback publishes the +TZ, whatever that was


def randomstr(length: int = 16, lowercase: bool = False) -> str:
    """
    :param length: The length of the random string
    :param lowercase: Whether to force the string to lowercase
    :returns: A random string of letters and numbers based on `length` and `lowercase`
    """
    letters = string.ascii_uppercase
    if lowercase:
        letters = string.ascii_lowercase
    return str(''.join(random.choices(letters + string.digits, k=length)))


def doc_generator(
    count: int = 10, start_at: int = 0, match: bool = True
) -> t.Generator[t.Dict, None, None]:
    """
    :param count: Create this many docs
    :param start_at: Append value starts with this value
    :param match: Do we want fieldnames to match between docgen runs, or be random?
        Also affects document document field "number" (value will be incremental if
        match is True, a random value between 1001 and 32767 if False)
    :returns: A generator shipping docs
    """
    keys = ['message', 'nested', 'deep']
    # Start with an empty map
    matchmap = {}
    # Iterate over each key
    for key in keys:
        # If match is True
        if match:
            # Set matchmap[key] to key
            matchmap[key] = key
        else:
            # Otherwise matchmap[key] will have a random string value
            matchmap[key] = randomstr()

    # This is where count and start_at matter
    for num in range(start_at, start_at + count):
        yield {
            '@timestamp': iso8601_now(),
            'message': f'{matchmap["message"]}{num}',  # message# or randomstr#
            'number': (
                num if match else random.randint(1001, 32767)
            ),  # value of num or random int
            'nested': {'key': f'{matchmap["nested"]}{num}'},  # nested#
            'deep': {'l1': {'l2': {'l3': f'{matchmap["deep"]}{num}'}}},  # deep#
        }
