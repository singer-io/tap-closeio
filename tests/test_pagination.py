from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import CloseioBase


class CloseioPaginationTest(PaginationTest, CloseioBase):
    """Closeio pagination test implementation """

    start_date = '2016-07-01T00:00:00Z'

    @staticmethod
    def name():
        return "tt_closeio_pagination"

    #Include the other streams that have records
    def streams_to_test(self):
        # there is not enough data for any of these streams to paginate
        # activities and event_log use a different pagination scheme than the other streams
        # All other streams have standard pagination of 100 records per page.
        return self.expected_stream_names().difference({
            'custom_fields',
            'users',
            'event_log'})
