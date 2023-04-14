import unittest
from base import CloseioBase
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

class CloseioBookmarkTest(BookmarkTest, CloseioBase):
    """Closeio bookmark test implementation"""


    @staticmethod
    def name():
        return "tt_closeio_bookmark"

    def streams_to_test(self):
        streams_to_exclude = {'event_log', 'custom_fields'}
        return set(self.expected_metadata().keys()) - streams_to_exclude

    def manipulate_state(self, old_state):
        manipulated_state = {
        'bookmarks': {
            stream: { self.get_replication_key_for_stream(stream): self.bookmark_date}
            for stream in old_state['bookmarks'].keys()
            }
          }
        return manipulated_state

    @unittest.skip("Does not apply")
    def test_sync_2_bookmark_greater_than_sync_1(self):
        """
        This test is meant for taps where bookmark percision is fine enough to always get greater
        bookmark values between syncs.  This tap uses bookmark values based on the last record
        and does not get new data fast enough (or at all) to support this test.
        """

    # set default values for test in init
    def __init__(self, test_run):
        super().__init__(test_run)

        #Hard coded bookmark_date as there are no active records
        self.bookmark_date = '2018-01-25T00:00:00Z'
