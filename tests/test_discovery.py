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
    # All fields are automatic and the standard implementation fails.
    # The test is overridden to allow for this as it is expected behavior for this tap
    def test_non_automatic_fields_by_streams(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather expectations

                # gather results
                catalog = [catalog for catalog in self.found_catalogs
                           if catalog["stream_name"] == stream][0]
                schema_and_metadata = menagerie.get_annotated_schema(
                    self.conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                inclusions_other_than_automatic = {
                    item.get("metadata", ["inclusion", None]).get("inclusion") for item in metadata
                    if item.get("breadcrumb", []) != []
                    and item.get("metadata").get("inclusion") != "automatic"}

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                with self.subTest(msg="validating automatic fields"):
                    self.assertEqual(len(inclusions_other_than_automatic), 0,
                                     logging="verifying if there are any non-automatic fields")
