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
                'TAP_CLOSEIO_API_KEY', 
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))


    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################
    #TODO refactor the below method to address the attribution window issue for activites stream
    # def get_sync_start_time(self, stream, bookmark):
    #     """
    #     Calculates the sync start time, with respect to the lookback window
    #     """
    #     conversion_day = dt.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) - timedelta(days=self.lookback_window)
    #     bookmark_datetime = dt.strptime(bookmark, self.BOOKMARK_FORMAT)
    #     start_date_datetime = dt.strptime(self.start_date, self.START_DATE_FORMAT)
    #     return  min(bookmark_datetime, max(start_date_datetime, conversion_day))

