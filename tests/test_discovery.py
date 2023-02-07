import unittest

from tap_tester import menagerie, connections
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import CloseioBase


class CloseioDiscoveryTest(DiscoveryTest, CloseioBase):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_closeio_discovery"

    def streams_to_test(self):
        return set(self.expected_metadata().keys())
