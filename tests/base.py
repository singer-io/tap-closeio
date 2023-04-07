"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import os
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import connections, menagerie, runner, LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class CloseioBase(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """


    REPLICATION_KEY_FORMAT = "%Y-%m-%dT00:00:00.000000Z"
    BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    PAGE_SIZE = 100000
    start_date = ""


    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-closeio"


    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.closeio"


    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            # added defualt value as there is no current activity
            'start_date':'2015-03-25T00:00:00Z', # '2018-03-25T00:00:00Z' for faster test runs
             'api_key':os.getenv('TAP_CLOSEIO_API_KEY'),

        }

        if original:
            return return_value

        if self.start_date:
            return_value["start_date"] = self.start_date
        return return_value


    @staticmethod
    def get_credentials():
        return {
            'api_key':os.getenv('TAP_CLOSEIO_API_KEY'),
        }


    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        default_expectations = {
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.REPLICATION_KEYS: {"date_updated"},
            self.RESPECTS_START_DATE: True,
        }

        return {
            'activities': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_created"},
                self.RESPECTS_START_DATE: True,
            },
            'custom_fields': default_expectations,
            'event_log': default_expectations,
            'leads': default_expectations,
            'tasks': default_expectations,
            'users': default_expectations
        }

    def expected_automatic_fields(self):
        auto_fields ={
            'activities': {
                    'need_smtp_credentials',
                    'cc',
                    'send_attempts',
                    'date_sent',
                    'envelope',
                    'references',
                    'updated_by',
                    'id',
                    'direction',
                    'user_id',
                    'subject',
                    'template_name',
                    'body_html',
                    'status',
                    'opens',
                    'thread_id',
                    'lead_id',
                    'users',
                    'attachments',
                    'body_text_quoted',
                    'to',
                    'created_by_name',
                    'body_html_quoted',
                    'in_reply_to_id',
                    'body_preview',
                    'email_account_id',
                    'date_scheduled',
                    'template_id',
                    'user_name',
                    'date_created',
                    'date_updated',
                    'created_by',
                    'organization_id',
                    'body_text',
                    'opens_summary',
                    '_type',
                    'sender',
                    'updated_by_name',
                    'bcc',
                    'message_ids',
                    'contact_id',
                    'new_status_label',
                    'task_id',
                    'new_status_id',
                    'opportunity_date_won',
                    'opportunity_value_currency',
                    'transferred_from',
                    'task_assigned_to',
                    'source',
                    'task_text',
                    'new_status_type',
                    'voicemail_url',
                    'opportunity_confidence',
                    'task_assigned_to_name',
                    'note',
                    'import_id',
                    'opportunity_value_formatted',
                    'opportunity_value_period',
                    'voicemail_duration',
                    'recording_url',
                    'old_status_label',
                    'opportunity_id',
                    'local_phone',
                    'old_status_id',
                    'old_status_type',
                    'opportunity_value',
                    'duration',
                    'dialer_id',
                    'remote_phone',
                    'transferred_to',
                    'phone',
                },
    'custom_fields': {
                    'date_created',
                    'date_updated',
                    'editable_with_roles',
                    'id',
                    'organization_id',
                    'name',
                    'choices',
                    'created_by',
                    'type',
                    'updated_by',
                },
            'event_log': {
                    'date_updated',
                    'id',
                    'previous_data',
                    'date_created',
                    'user_id',
                    'data',
                    'lead_id',
                    'action',
                    'object_id',
                    'organization_id',
                    'request_id',
                    'meta',
                    'changed_fields',
                    'object_type',
                },
            'leads': {
                    'date_updated',
                    'display_name',
                    'addresses',
                    'date_created',
                    'id',
                    'status_id',
                    'opportunities',
                    'organization_id',
                    'name',
                    'tasks',
                    'created_by',
                    'status_label',
                    'integration_links',
                    'contacts',
                    'custom_fields',
                    'url',
                    'html_url',
                    'description',
                    'updated_by_name',
                    'created_by_name',
                    'updated_by',
                },
            'tasks': {
                    'date_created',
                    'date_updated',
                    'id',
                    'is_complete',
                    'phone',
                    'local_phone',
                    'voicemail_url',
                    'organization_id',
                    '_type',
                    'date',
                    'voicemail_duration',
                    'opportunity_value_currency',
                    'contact_name',
                    'body_preview',
                    'phone_number_description',
                    'text',
                    'opportunity_note',
                    'due_date',
                    'updated_by',
                    'opportunity_value',
                    'email_id',
                    'remote_phone',
                    'created_by',
                    'view',
                    'object_type',
                    'subject',
                    'remote_phone_description',
                    'contact_id',
                    'emails',
                    'lead_id',
                    'recording_url',
                    'remote_phone_formatted',
                    'assigned_to',
                    'is_dateless',
                    'object_id',
                    'opportunity_value_formatted',
                    'updated_by_name',
                    'phone_formatted',
                    'created_by_name',
                    'lead_name',
                    'assigned_to_name',
                    'opportunity_value_period',
                },
            'users': {
                    'date_updated',
                    'id',
                    'last_name',
                    'date_created',
                    'organizations',
                    'email',
                    'first_name',
                    'image',
                    'last_used_timezone',
                },
        }
        return auto_fields


    @classmethod
    def setUpClass(cls):
        super().setUpClass(logging="Ensuring environment variables are sourced.")
        missing_envs = [
            x for x in [
                'TAP_CLOSEIO_API_KEY' #TODO add account_id and password if needed(credentials)
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))


    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################


    @staticmethod
    def expected_pagination_fields(): # TODO does this apply?
        return {
            "Test Report 1" : set(),
            "Audience Overview": {
                "ga:users", "ga:newUsers", "ga:sessions", "ga:sessionsPerUser", "ga:pageviews",
                "ga:pageviewsPerSession", "ga:sessionDuration", "ga:bounceRate", "ga:date",
                # "ga:pageviews",
            },
            "Audience Geo Location": set(),
            "Audience Technology": set(),
            "Acquisition Overview": set(),
            "Behavior Overview": set(),
            "Ecommerce Overview": set(),
        }


    # TODO refactor code to remove this method if possible, ga4 relic
    def get_stream_name(self, stream):
        """
        Returns the stream_name given the tap_stream_id because synced_records
        from the target output batches records by stream_name

        Since the GA4 tap_stream_id is a UUID instead of the usual case of
        tap_stream_id == stream_name, we need to get the stream_name that
        maps to tap_stream_id
        """
        stream_name=stream
        # custom_reports_names_to_ids().get(tap_stream_id, tap_stream_id)
        return stream_name

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id, test_catalogs, select_all_fields )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)


    def get_sync_start_time(self, stream, bookmark):
        """
        Calculates the sync start time, with respect to the lookback window
        """
        conversion_day = dt.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=self.lookback_window)
        bookmark_datetime = dt.strptime(bookmark, self.BOOKMARK_FORMAT)
        start_date_datetime = dt.strptime(self.start_date, self.START_DATE_FORMAT)
        return  min(bookmark_datetime, max(start_date_datetime, conversion_day))


    # TODO is this still useful now that we have get_stream_name?
    def get_record_count_by_stream(self, record_count, stream):
        count = record_count.get(stream)
        if not count:
            stream_name = self.custom_reports_names_to_ids().get(stream)
            return record_count.get(stream_name)
        return count


    def get_bookmark_value(self, state, stream):
        bookmark = state.get('bookmarks', {})
        stream_bookmark = bookmark.get(stream)
        stream_replication_key = self.expected_metadata().get(stream).get(self.REPLICATION_KEYS)
        if stream_bookmark:
            return stream_bookmark.get(stream_replication_key.pop())
        return None

#    def test_sync_2_bookmark_greater_than_sync_1(self):
#        """
#        Compares bookmark values of both syncs if bookmark values are
#        precise enough to always get a greater value in the second sync
#
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
#                bookmark_value_1 = self.get_bookmark_value(self.state_1, stream)
#                bookmark_value_2 = self.get_bookmark_value(self.state_2, stream)
#
#                # Verify second sync bookmark is equal or greater than the
#                # first sync bookmark
#                parsed_bookmark_value_1 = self.parse_date(bookmark_value_1)
#                parsed_bookmark_value_2 = self.parse_date(bookmark_value_2)
#                self.assertGreater(parsed_bookmark_value_2, parsed_bookmark_value_1)
#
