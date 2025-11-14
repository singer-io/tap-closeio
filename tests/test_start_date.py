from tap_tester.base_suite_tests.start_date_test import StartDateTest
from base import CloseioBase


class CloseioStartDateTest(StartDateTest, CloseioBase):
    """Standard Start Date Test"""

    @staticmethod
    def name():
        return "tt_closeio_start_date"

    def streams_to_test(self):
        return self.expected_stream_names().difference({
            'tasks',
            'custom_fields',
            'users',
        })

    @property
    def start_date_1(self):
        return '2025-01-01T00:00:00Z'

    @property
    def start_date_2(self):
        return '2025-11-01T00:00:00Z'
