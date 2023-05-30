"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from base import CloseioBase


class CloseioAllFieldsTest(AllFieldsTest, CloseioBase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    selected_fields = AllFieldsTest.selected_fields

    @staticmethod
    def name():
        return "tt_closeio_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {'event_log', 'custom_fields'}
        return self.expected_stream_names().difference(streams_to_exclude)

    def test_all_fields_for_streams_are_replicated(self):
        self.selected_fields = self.too_many_fields_replicated()
        super().test_all_fields_for_streams_are_replicated()
