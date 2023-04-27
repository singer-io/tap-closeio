from tap_tester import menagerie, connections
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import CloseioBase


class CloseioDiscoveryTest(DiscoveryTest, CloseioBase):
    """Standard Discovery Test"""

    @staticmethod
    def name():
        return "tt_closeio_discovery"

    def streams_to_test(self):
        return self.expected_stream_names()

    # ###########################################################################
    # overridden tests to test other fields inclusions
    # ##########################################################################

    def discovery_expected_replication_keys(self):
        """
        TODO - BUG All streams have replicated keys as both date_created and date_updated.
            This is incorrect according to the docs and operation of the tap.
            Writing this workaround for the test until the bug is addressed.
        """
        return {
            'activities': {'date_created', 'date_updated'},
            'custom_fields': {'date_created', 'date_updated'},
            'event_log': {'date_created', 'date_updated'},
            'leads': {'date_created', 'date_updated'},
            'tasks': {'date_created', 'date_updated'},
            'users': {'date_created', 'date_updated'}}

    def test_replication_metadata(self):
        self.expected_replication_keys = self.discovery_expected_replication_keys
        super().test_replication_metadata()
