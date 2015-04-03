from __future__ import absolute_import
from datetime import datetime

EPOCH = datetime(year=1970, month=1, day=1)


def epoch(input=None):
    if input is None:
        input = datetime.utcnow()
    return (input - EPOCH).total_seconds()
