from datetime import datetime, timedelta
import unittest
from unittest import mock
from tap_closeio.streams import sync_activities
from tap_closeio.context import Context

@mock.patch("tap_closeio.streams.paginated_sync")
class TestActivityStreamDateWindow(unittest.TestCase):

    def test_activity_stream_default_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when no "date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d/"), # set date 40 days later than now
            "api_key": "test_API_key"
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 3 times as the start date is 40 days later and default date window is 15 days
        self.assertEqual(mocked_paginated_sync.call_count, 3)

    @mock.patch("tap_closeio.streams.LOGGER.warning")
    def test_activity_stream_empty_string_date_window(self, mocked_logger_warning, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when empty string "date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "date_window": ""
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set default date window
        self.assertEqual(mocked_paginated_sync.call_count, 3)
        # verify warning is raised for invalid date window
        mocked_logger_warning.assert_called_with("Invalid value of date window is passed: '', using default window size of 15 days.")

    @mock.patch("tap_closeio.streams.LOGGER.warning")
    def test_activity_stream_0_date_window(self, mocked_logger_warning, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when int 0 "date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "date_window": 0
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set default date window
        self.assertEqual(mocked_paginated_sync.call_count, 3)
        # verify warning is raised for invalid date window
        mocked_logger_warning.assert_called_with("Invalid value of date window is passed: '0', using default window size of 15 days.")

    @mock.patch("tap_closeio.streams.LOGGER.warning")
    def test_activity_stream_string_0_date_window(self, mocked_logger_warning, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when string 0 "date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "date_window": "0"
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set default date window
        self.assertEqual(mocked_paginated_sync.call_count, 3)
        # verify warning is raised for invalid date window
        mocked_logger_warning.assert_called_with("Invalid value of date window is passed: '0', using default window size of 15 days.")

    def test_activity_stream_configurable_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in the desired date window passed in "date_window" config value
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "date_window": 3
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set date window as 3 days
        self.assertEqual(mocked_paginated_sync.call_count, 14)
