"""
Test that with no fields selected for a stream automatic fields are still replicated
"""

from tap_tester import runner, menagerie, connections
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from tap_tester.logger import LOGGER
from base import CloseioBase

class CloseioAllFieldsTest(AllFieldsTest,CloseioBase):
    """Test that with no fields selected for a stream automatic fields are still replicated"""


    @staticmethod
    def name():
        return "tt_closeio_all_fields_test"
    def streams_to_test(self):
        streams_to_exclude = {'event_log', 'custom_fields'}
        return set(self.expected_metadata().keys()) - streams_to_exclude
