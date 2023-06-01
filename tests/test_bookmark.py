from base import CloseioBase
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class CloseioBookmarkTest(BookmarkTest, CloseioBase):
    """Closeio bookmark test implementation"""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    initial_bookmarks = {
            'bookmarks': {
                'custom_fields': {'date_updated': '2016-07-07T14:22:04.624000Z'},
                'leads': {'date_updated': '2020-05-06T18:54:08.250000Z'},
                'activities': {'date_created': '2020-11-06T20:17:17Z'},
                "tasks": {'date_updated': '2018-01-17T13:45:25.125000Z'},
                'users': {'date_updated': '2019-10-10T03:23:15.466000Z'}}}

    @staticmethod
    def name():
        return "tt_closeio_bookmark"

    def streams_to_test(self):
        # TODO - are there no records for the event_log stream?
        # return {'tasks'}
        return self.expected_stream_names().difference({'event_log'})

    def test_first_sync_bookmark(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'activities'})
        # hide and override streams to test for this test.
        self.streams_to_test = bookmark_exceptions  # pylint: disable=method-hidden
        super().test_first_sync_bookmark()

    def test_second_sync_bookmark(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'activities'})
        # hide and override streams to test for this test.
        self.streams_to_test = bookmark_exceptions  # pylint: disable=method-hidden
        super().test_second_sync_bookmark()

    def test_sync_2_bookmark_greater_or_equal_to_sync_1(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'activities'})
        # TODO - activities bookmarks don't conform to the latest record, nor today.
        #   Look more into what is going on here and if this is a bug.  Nor is the bookmark
        #   consistent between syncs.
        self.streams_to_test = bookmark_exceptions
        super().test_sync_2_bookmark_greater_or_equal_to_sync_1()

    def test_bookmark_format(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'activities'})
        # hide and override streams to test for this test.
        self.streams_to_test = bookmark_exceptions  # pylint: disable=method-hidden
        super().test_bookmark_format()

    def test_bookmark_format_activities(self):
        # TODO - why is this bookmark different than all other bookmark formats?
        self.streams_to_test = lambda : {'activities'}
        self.bookmark_format = "%Y-%m-%dT%H:%M:%S+00:00"
        BookmarkTest.test_bookmark_format(self)
