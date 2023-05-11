from tap_tester.base_suite_tests.start_date_test import StartDateTest
from base import CloseioBase


class CloseioStartDateTest(StartDateTest, CloseioBase):
    """Standard Start Date Test"""

    @staticmethod
    def name():
        return "tt_closeio_start_date"

    def streams_to_test(self):
        return self.expected_stream_names().difference({
            'users',  # TODO to set date ranges between 2019 and 2020
            'activities',  # TODO - BUG lookback window being applied and shouldn't be
                           #    https://talend.slack.com/archives/C01DR0Q8DUH/p1683739644264939
            'event_log',  # TODO Sync1 and Sync2 have same amount of data
        })

    @property
    def start_date_1(self):
        return '2015-03-25T00:00:00Z'  # '2020-03-25T00:00:00Z'use these values for debug mode

    @property
    def start_date_2(self):
        return '2017-01-25T00:00:00Z'  # '2017-08-11T00:00:00Z'use these values for debug mode
