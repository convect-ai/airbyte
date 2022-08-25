#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from typing import Iterable

import pendulum
from pendulum import DateTime, Period


def chunk_date_range(start_date: DateTime, interval=pendulum.duration(days=1)) -> Iterable[Period]:
    """
    Yields a list of the beginning and ending timestamps of each day between the start date and now.
    The return value is a pendulum.period
    """

    now = pendulum.now()
    # Each stream_slice contains the beginning and ending timestamp for a 24 hour period
    while start_date <= now:
        end_date = start_date + interval
        yield pendulum.period(start_date, end_date)
        start_date = end_date
