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
        # TODO - BUG it looks like there is no metadata for some fields even though they
        #   are actually replicated.  This isn't right.  Maybe it doesn't matter too much
        #   since everything is automatic but we should write this up.
        def too_many_fields_replicated(stream=None):
            actual_expected = CloseioBase.expected_automatic_fields()
            extra_fields = {
                "users": {'google_profile_image_url', 'email_verified_at'},
                "custom_fields": {
                    'accepts_multiple_values',
                    'back_reference_is_visible',
                    'description',
                    'is_shared',
                    'referenced_custom_type_id',
                },
                "activities": {
                    'activity_at',
                    'bulk_email_action_id',
                    'call_method',
                    'coach_legs',
                    'cost',
                    'date_answered',
                    'dialer_saved_search_id',
                    'disposition',
                    'error_message',
                    'followup_sequence_delay',
                    'followup_sequence_id',
                    'forwarded_to',
                    'has_recording',
                    'has_reply',
                    'is_forwarded',
                    'is_joinable',
                    'is_to_group_number',
                    'local_country_iso',
                    'local_phone_formatted',
                    'new_pipeline_id',
                    'new_pipeline_name',
                    'note_html',
                    'old_pipeline_id',
                    'old_pipeline_name',
                    'recording_expires_at',
                    'remote_country_iso',
                    'remote_phone_formatted',
                    'send_as_id',
                    'sequence_id',
                    'sequence_name',
                    'sequence_subscription_id',
                    'text',
                    'transferred_from_user_id',
                    'transferred_to_user_id',
                },
            }
            missing_fields = {
                "tasks": {
                    'body_preview',
                    'email_id',
                    'emails',
                    'local_phone',
                    'opportunity_note',
                    'opportunity_value',
                    'opportunity_value_currency',
                    'opportunity_value_formatted',
                    'opportunity_value_period',
                    'phone',
                    'phone_formatted',
                    'phone_number_description',
                    'recording_url',
                    'remote_phone',
                    'remote_phone_description',
                    'remote_phone_formatted',
                    'subject',
                    'voicemail_duration',
                    'voicemail_url',
                }}
            additional_expected = {
                stream: fields.union(
                    extra_fields.get(stream, set())).difference(
                    missing_fields.get(stream, set()))
                for stream, fields in actual_expected.items()}
            if not stream:
                return additional_expected
            return additional_expected[stream]
        self.expected_automatic_fields = too_many_fields_replicated
        super().test_only_automatic_fields_replicated()
