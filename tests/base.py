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
    BOOKMARK_FORMAT = "%Y-%m-%d"
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
                self.AUTOMATIC_FIELDS: {
                    'date_updated',
                    'id',
                    'opportunity_value',
                    'organization_id',
                    'user_name',
                    'opens_summary',
                    'source',
                    'opportunity_date_won',
                    'updated_by',
                    'task_assigned_to_name',
                    'contact_id',
                    'user_id',
                    'email_account_id',
                    'old_status_label',
                    'template_id',
                    'opportunity_confidence',
                    'opportunity_value_period',
                    '_type',
                    'duration',
                    'sender',
                    'recording_url',
                    'bcc',
                    'transferred_to',
                    'body_text',
                    'envelope',
                    'status',
                    'opportunity_id',
                    'task_assigned_to',
                    'voicemail_url',
                    'template_name',
                    'users',
                    'dialer_id',
                    'date_created',
                    'attachments',
                    'direction',
                    'local_phone',
                    'opportunity_value_formatted',
                    'transferred_from',
                    'date_scheduled',
                    'body_text_quoted',
                    'lead_id',
                    'opens',
                    'old_status_id',
                    'body_html',
                    'new_status_id',
                    'thread_id',
                    'remote_phone',
                    'body_preview',
                    'created_by_name',
                    'subject',
                    'message_ids',
                    'references',
                    'to',
                    'new_status_type',
                    'note',
                    'date_sent',
                    'cc',
                    'need_smtp_credentials',
                    'phone',
                    'body_html_quoted',
                    'opportunity_value_currency',
                    'task_id',
                    'new_status_label',
                    'in_reply_to_id',
                    'task_text',
                    'import_id',
                    'send_attempts',
                    'updated_by_name',
                    'created_by',
                    'old_status_type',
                    'voicemail_duration',
                },
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_created"},
                self.RESPECTS_START_DATE: True,
            },
            'custom_fields': {
                self.AUTOMATIC_FIELDS: {
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
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.RESPECTS_START_DATE: True,
            },

            'event_log': {
                self.AUTOMATIC_FIELDS: {
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
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.RESPECTS_START_DATE: True,
            },

            'leads': {
                self.AUTOMATIC_FIELDS: {
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
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.RESPECTS_START_DATE: True,
            },

            'tasks': {
                self.AUTOMATIC_FIELDS: {
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
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.RESPECTS_START_DATE: True,
            },

            'users': {
                self.AUTOMATIC_FIELDS: {
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
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.RESPECTS_START_DATE: True,
            }

        }

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.AUTOMATIC_FIELDS, set())

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
        # custom_reports_names_to_ids().get(tap_stream_id, tap_stream_id)
        return self.stream


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)


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
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

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
        stream_replication_key = self.expected_metadata().get(stream,set()).get('REPLICATION_KEYS')
        if stream_bookmark:
            return stream_bookmark.get(stream_replication_key)
        return None
