from base import CloseioBase
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class CloseioBookmarkTest(BookmarkTest, CloseioBase):
    """Closeio bookmark test implementation"""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    initial_bookmarks = {
        'bookmarks': {
            'custom_fields': {'date_updated': '2025-01-01T00:00:00+00:00'},
            'leads': {'date_updated': '2025-01-01T00:00:00+00:00'},
            'activities': {'date_created': '2025-01-01T00:00:00+00:00'},
            "tasks": {'date_updated': '2025-01-01T00:00:00+00:00'},
        }}

    @staticmethod
    def name():
        return "tt_closeio_bookmark"

    def streams_to_test(self):
        # TODO - are there no records for the event_log stream?
        # return {'tasks'}
        return self.expected_stream_names().difference({'event_log', 'users'})

    def test_first_sync_bookmark(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'users'})
        # hide and override streams to test for this test.
        self.streams_to_test = bookmark_exceptions  # pylint: disable=method-hidden
        super().test_first_sync_bookmark()

    def test_second_sync_bookmark(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'users'})
        # hide and override streams to test for this test.
        self.streams_to_test = bookmark_exceptions  # pylint: disable=method-hidden
        super().test_second_sync_bookmark()

    def test_sync_2_bookmark_greater_or_equal_to_sync_1(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'users'})
        self.streams_to_test = bookmark_exceptions
        super().test_sync_2_bookmark_greater_or_equal_to_sync_1()

    def test_bookmark_format(self):
        def bookmark_exceptions():
            return self.expected_stream_names().difference({'event_log', 'users'})
        # hide and override streams to test for this test.
        self.streams_to_test = bookmark_exceptions  # pylint: disable=method-hidden
        super().test_bookmark_format()
