import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

from base import CloseioBase
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

class CloseioBookmarkTest(BookmarkTest, CloseioBase):
    """Closeio bookmark test implementation"""


    @staticmethod
    def name():
        return "tt_closeio_bookmark"

    def streams_to_test(self):
        expected_streams = self.expected_metadata().keys()
        return set(expected_streams).difference(['event_log', 'activities'])

    def manipulate_state(self, old_state):
        manipulated_state = {
        'bookmarks': {
            stream: { self.get_replication_key_for_stream(stream): self.bookmark_date}
            for stream in old_state['bookmarks'].keys()
            }
          }
        return manipulated_state


    # set default values for test in init
    def __init__(self, test_run):
        super().__init__(test_run)
        #        self.start_date = self.timedelta_formatted(dt.now(),
        #                                                   days=-1095,
        #                                                   date_format=self.START_DATE_FORMAT)

        self.bookmark_date = '2018-01-25T00:00:00Z'
        #self.bookmark_date = self.timedelta_formatted(dt.now(),
        #                                              days=-730,
        #                                              date_format=self.BOOKMARK_FORMAT)

    #skip test for sync2 bookmark >= sync1
    #def test_sync_2_bookmark_greater_than_sync_1(self):
    #    """
    #    Compares bookmark values of both syncs if bookmark values are
    #    precise enough to always get a greater value in the second sync
##
#        Skip if this is not the case
#
#        ex: bookmark format: YYYY-MM-DDTHH:MM:SS
#        """
#        for stream in self.streams_to_test():
#            with self.subTest(stream=stream):
#                # gather results
#                stream_bookmark_1 = self.bookmarks_1.get(stream)
#                stream_bookmark_2 = self.bookmarks_2.get(stream)
#
#                bookmark_value_1 = self.get_bookmark_value(self.bookmark_date, stream)
#                #Refer netsuite to find the min bookmark value from synced records of second sync
#                bookmark_value_2 = self.get_bookmark_value(self.state_2, stream)
#
#                # Verify second sync bookmark is equal or greater than the
#                # first sync bookmark
#                parsed_bookmark_value_1 = self.parse_date(bookmark_value_1)
#                parsed_bookmark_value_2 = self.parse_date(bookmark_value_2)
#                self.assertGreater(parsed_bookmark_value_2, parsed_bookmark_value_1)
#
