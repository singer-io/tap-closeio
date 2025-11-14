from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import CloseioBase


class CloseioPaginationTest(PaginationTest, CloseioBase):
    """Closeio pagination test implementation """

    @staticmethod
    def name():
        return "tt_closeio_pagination"

    #Include the other streams that have records
    def streams_to_test(self):
        # there is not enough data for any of these streams to paginate
        return {'leads'}
