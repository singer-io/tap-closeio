from tap_tester.base_suite_tests.start_date_test import StartDateTest
from base import CloseioBase


class CloseioStartDateTest(StartDateTest, CloseioBase):
    """Standard Start Date Test"""

    @staticmethod
    def name():
        return "tt_closeio_start_date"

    def streams_to_test(self):
        return {#'users',  # TODO to set date ranges between 2019 and 2020
                #'activities',  # TODO spike on how to handle 24h attribution window
                'leads',
                'tasks',
                'custom_fields',
                #'event_log',  # TODO Sync1 and Sync2 have same amount of data
                }


    # set default values for test in init
    def __init__(self, test_run):
        super().__init__(test_run)
        self.start_date_1 ='2015-03-25T00:00:00Z' #'2020-03-25T00:00:00Z'use these values for debug mode
        self.start_date_2 ='2017-01-25T00:00:00Z'  #'2017-08-11T00:00:00Z'use these values for debug mode
