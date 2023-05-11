from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest

from base import CloseioBase


class CloseioSyncCanaryTest(SyncCanaryTest, CloseioBase):
    """Standard Sync Canary Test"""

    @staticmethod
    def name():
        return "tt_closeio_sync"

    def streams_to_test(self):
        # We have no test data for the event_log stream
        return self.expected_stream_names().difference({'event_log'})

    # update the start date for this test
    def setUp(self):
        self.start_date = '2018-01-28T00:00:00Z'
        super().setUp()
