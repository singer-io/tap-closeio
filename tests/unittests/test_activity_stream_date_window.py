from datetime import datetime, timedelta
import unittest
from unittest import mock
from tap_closeio.streams import sync_activities
from tap_closeio.context import Context

@mock.patch("tap_closeio.streams.paginated_sync")
class TestActivityStreamDateWindow(unittest.TestCase):

    def test_activity_stream_default_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when no "activities_date_window" is passed in the config
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

    def test_activity_stream_empty_string_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when empty string "activities_date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "activities_date_window": ""
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set default date window
        self.assertEqual(mocked_paginated_sync.call_count, 3)

    def test_activity_stream_0_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when int 0 "activities_date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "activities_date_window": 0
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set default date window
        self.assertEqual(mocked_paginated_sync.call_count, 3)

    def test_activity_stream_string_0_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in 15 days window (default) when string 0 "activities_date_window" is passed in the config
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "activities_date_window": "0"
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set default date window
        self.assertEqual(mocked_paginated_sync.call_count, 3)

    def test_activity_stream_configurable_date_window(self, mocked_paginated_sync):
        """
            Test case to verify we are calling Activity API in the desired date window passed in "activities_date_window" config value
        """
        # now date
        now_date = datetime.now()
        config = {
            "start_date": (now_date - timedelta(days=40)).strftime("%Y-%m-%d"), # set date 40 days later than now
            "api_key": "test_API_key",
            "activities_date_window": 3
        }
        state = {}

        # create 'Context' object
        ctx = Context(config, state)

        # function call
        sync_activities(ctx)

        # verify we called 'paginated_sync' 14 times as the start date is 40 days later and we have set date window as 3 days
        self.assertEqual(mocked_paginated_sync.call_count, 14)
