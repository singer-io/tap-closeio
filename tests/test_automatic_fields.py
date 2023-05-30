from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import CloseioBase


class CloseioMinimumSelectionTest(MinimumSelectionTest, CloseioBase):
    """
    Standard Automatic Fields test

    NOTE: Since all fields are automatic this test probably isn't actually validating anything
    """

    @staticmethod
    def name():
        return "tt_closeio_auto"

    def streams_to_test(self):
        return self.expected_stream_names().difference({'event_log'})

    def test_only_automatic_fields_replicated(self):
        self.expected_automatic_fields = self.too_many_fields_replicated
        super().test_only_automatic_fields_replicated()
