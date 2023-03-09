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
    #return set(self.expected_metadata().keys(){'activities'}
    ############################################################################
    #overridden tests to test other fields inclusioons
    ###########################################################################

    def test_inclusion_by_streams(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):

                # gather expectations
                expected_automatic_fields = self.expected_automatic_fields()[stream]

                # gather results
                catalog = [catalog for catalog in self.found_catalogs
                           if catalog["stream_name"] == stream][0]
                schema_and_metadata = menagerie.get_annotated_schema(self.conn_id, catalog['stream_id'])
                metadata = schema_and_metadata["metadata"]
                stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                actual_automatic_fields = {item.get("breadcrumb", ["properties", None])[1]
                                           for item in metadata
                                           if item.get("metadata").get("inclusion") == "automatic"}

                # verify that primary, replication are given the inclusion of automatic in metadata.
                with self.subTest(msg="validating automatic fields"):
                    self.assertSetEqual(expected_automatic_fields, actual_automatic_fields,
                        logging="verify primary and replication key fields are automatic"
                    )

                # verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                inclusions_other_than_automatic = {
                    item.get("metadata").get("inclusion") for item in metadata
                    if item.get("breadcrumb", []) != []
                    and item.get("breadcrumb", ["properties", None])[1] not in actual_automatic_fields
                }


                with self.subTest(msg="validating automatic fields"):
                    self.assertEqual(len(inclusions_other_than_automatic), 0,
                        msg="Not all non key properties are set to 'available' in metadata",
                        logging="verify all non-automatic fields are available")
