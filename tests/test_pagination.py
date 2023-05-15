import os
from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import CloseioBase

class CloseioPaginationTest(PaginationTest, CloseioBase):
    """Closeio pagination test implementation """
    #tdl-22959 - Match with the per_page limit in tap-closeio/tap_closeio/http.py
    PAGE_SIZE=100

    @staticmethod
    def name():
        return "tt_closeio_pagination"

    #Include the other streams that have records
    def streams_to_test(self):
        return { 'activities', 'leads', 'tasks' }

    def streams_to_selected_fields(self):
        return {
            "leads": {
                "date_created",
                "contacts",
            },
            "activities": {
                "date_created",
                "user_name",
            },
            "tasks": {
                "date_created",
                "name",
            },
        }

    def get_page_limit_for_stream(self, stream):
        return self.PAGE_SIZE

    def __init__(self, test_run):
        super().__init__(test_run)
        self.start_date='2015-03-25T00:00:00Z'
