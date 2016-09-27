#!/usr/bin/env python

from datetime import datetime

# a processor converts original value to be a minutely increment value for counter-like metrics


class CounterPorcessor(object):
    def __init__(self):
        self.last_timedate_count = None
        self.curr_timedate = datetime.now()
        self.curr_timedate_count = None

    def process_counter(self, ts, val):
        curr_datetime = datetime.fromtimestamp(ts)
        if self.__within_same_minute(self.curr_timedate, curr_datetime):
            self.curr_timedate_count = val
        else:
            # now we cross minute boundary
            self.last_timedate_count = self.curr_timedate_count
            self.curr_timedate = curr_datetime

        if self.last_timedate_count is None:
            return_val = 0
        else:
            return_val = val - self.last_timedate_count
        return return_val

    def __within_same_minute(self, old_datetime, new_datetime):
        return old_datetime.year == new_datetime.year and old_datetime.month == new_datetime.month\
               and old_datetime.day == new_datetime.day  and old_datetime.hour == new_datetime.hour \
               and old_datetime.minute == new_datetime.minute