"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import os
from datetime import timedelta

from tap_tester.base_suite_tests.base_case import BaseCase


class CloseioBase(BaseCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """

    # REPLICATION_KEY_FORMAT = "%Y-%m-%dT00:00:00.000000Z"
    # BOOKMARK_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"
    # PAGE_SIZE = 100000

    # set the default start date which can be overridden in the tests,
    # by setting the property or changing self.start_date in a test.
    start_date ='2016-07-07T00:00:00Z'

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-closeio"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.closeio"

    def get_properties(self):
        """Configuration properties required for the tap."""

        return {
            # added defualt value as there is no current activity
            'start_date': self.start_date,  # '2018-03-25T00:00:00Z' for faster test runs
            'api_key': os.getenv('TAP_CLOSEIO_API_KEY')
        }

    @staticmethod
    def get_credentials():
        return {
            'api_key': os.getenv('TAP_CLOSEIO_API_KEY'),
        }

    @staticmethod
    def expected_metadata():
        """The expected streams and metadata about the streams"""
        default_expectations = {
            BaseCase.PRIMARY_KEYS: {"id"},
            BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
            BaseCase.REPLICATION_KEYS: {"date_updated"},
            BaseCase.RESPECTS_START_DATE: True,
            BaseCase.API_LIMIT: 100
        }

        return {
            'activities': {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"date_created"},
                BaseCase.RESPECTS_START_DATE: True,
                BaseCase.LOOK_BACK_WINDOW: timedelta(hours=24),
                # date_window request default 15 days.
                # Make sure start date is before the date_window
                BaseCase.API_LIMIT: 100
            },
            'custom_fields': default_expectations,
            'event_log': default_expectations,
            'leads': default_expectations,
            'tasks': default_expectations,
            'users': default_expectations
        }

    @staticmethod
    def expected_automatic_fields():
        return {
            'activities': {
                '_type',
                # 'activity_at',
                'attachments',
                'bcc',
                'body_html',
                'body_html_quoted',
                'body_preview',
                'body_text',
                'body_text_quoted',
                # 'bulk_email_action_id',
                # 'call_method',
                'cc',
                # 'coach_legs',
                'contact_id',
                # 'cost',
                'created_by',
                'created_by_name',
                # 'date_answered',
                'date_created',
                'date_scheduled',
                'date_sent',
                'date_updated',
                'dialer_id',
                # 'dialer_saved_search_id',
                'direction',
                # 'disposition',
                'duration',
                'email_account_id',
                'envelope',
                # 'error_message',
                # 'followup_sequence_delay',
                # 'followup_sequence_id',
                # 'forwarded_to',
                # 'has_recording',
                # 'has_reply',
                'id',
                'import_id',
                'in_reply_to_id',
                # 'is_forwarded',
                # 'is_joinable',
                # 'is_to_group_number',
                'lead_id',
                # 'local_country_iso',
                'local_phone',
                # 'local_phone_formatted',
                'message_ids',
                'need_smtp_credentials',
                # 'new_pipeline_id',
                # 'new_pipeline_name',
                'new_status_id',
                'new_status_label',
                'new_status_type',
                'note',
                # 'note_html',
                # 'old_pipeline_id',
                # 'old_pipeline_name',
                'old_status_id',
                'old_status_label',
                'old_status_type',
                'opens',
                'opens_summary',
                'opportunity_confidence',
                'opportunity_date_won',
                'opportunity_id',
                'opportunity_value',
                'opportunity_value_currency',
                'opportunity_value_formatted',
                'opportunity_value_period',
                'organization_id',
                'phone',
                # 'recording_expires_at',
                'recording_url',
                'references',
                # 'remote_country_iso',
                'remote_phone',
                # 'remote_phone_formatted',
                # 'send_as_id',
                'send_attempts',
                'sender',
                # 'sequence_id',
                # 'sequence_name',
                # 'sequence_subscription_id',
                'source',
                'status',
                'subject',
                'task_assigned_to',
                'task_assigned_to_name',
                'task_id',
                'task_text',
                'template_id',
                'template_name',
                # 'text',
                'thread_id',
                'to',
                'transferred_from',
                # 'transferred_from_user_id',
                'transferred_to',
                # 'transferred_to_user_id',
                'updated_by',
                'updated_by_name',
                'user_id',
                'user_name',
                'users',
                'voicemail_duration',
                'voicemail_url',
            },
            'custom_fields': {
                # 'accepts_multiple_values',
                # 'back_reference_is_visible',
                'choices',
                'created_by',
                'date_created',
                'date_updated',
                # 'description',
                'editable_with_roles',
                'id',
                # 'is_shared',
                'name',
                'organization_id',
                # 'referenced_custom_type_id',
                'type',
                'updated_by',
            },
            'event_log': {
                'action',
                'changed_fields',
                'data',
                'date_created',
                'date_updated',
                'id',
                'lead_id',
                'meta',
                'object_id',
                'object_type',
                'organization_id',
                'previous_data',
                'request_id',
                'user_id',
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
                '_type',
                'assigned_to',
                'assigned_to_name',
                'body_preview',
                'contact_id',
                'contact_name',
                'created_by',
                'created_by_name',
                'date',
                'date_created',
                'date_updated',
                'due_date',
                'email_id',
                'emails',
                'id',
                'is_complete',
                'is_dateless',
                'lead_id',
                'lead_name',
                'local_phone',
                'object_id',
                'object_type',
                'opportunity_note',
                'opportunity_value',
                'opportunity_value_currency',
                'opportunity_value_formatted',
                'opportunity_value_period',
                'organization_id',
                'phone',
                'phone_formatted',
                'phone_number_description',
                'recording_url',
                'remote_phone',
                'remote_phone_description',
                'remote_phone_formatted',
                'subject',
                'text',
                'updated_by',
                'updated_by_name',
                'view',
                'voicemail_duration',
                'voicemail_url',
            },
            'users': {
                'date_created',
                'date_updated',
                'email',
                # 'email_verified_at',
                'first_name',
                # 'google_profile_image_url',
                'id',
                'image',
                'last_name',
                'last_used_timezone',
                'organizations',
            },
        }

    @classmethod
    def setUpClass(cls, logging="Ensuring environment variables are sourced."):
        super().setUpClass(logging=logging)
        missing_envs = [
            x for x in [
                'TAP_CLOSEIO_API_KEY', 
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise ValueError(f"Missing environment variables: {missing_envs}")
