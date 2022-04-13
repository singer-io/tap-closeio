import unittest
from unittest import mock
from tap_closeio import Context
from tap_closeio.streams import sync_activities
import datetime


class TestAPIReuqestHasWidowBoundaryParams(unittest.TestCase):
    @mock.patch('tap_closeio.streams.paginated_sync')
    @mock.patch('tap_closeio.Context.update_start_date_bookmark', side_effect= lambda x: '2010-01-01')
    @mock.patch('tap_closeio.streams.singer.utils.now', side_effect=lambda : datetime.datetime(2022, 4, 13))
    @mock.patch('tap_closeio.streams.create_request')
    def test_request_has_date_created__gt_and_date_created__lt(self, mocked_create_request, mocked_singer_utils_now, mocked_update_start_bookmark, mocked_paginated_sync):
        """
        To verify that create_request function params have date_created__gt and date_created__lt
        """
        ctx = Context({'start_date': "test", 'api_key': 'test'}, {})
        params = {'date_created__gt': '2009-12-31T00:00:00', 'date_created__lt': '2022-04-13T00:00:00'}
        sync_activities(ctx)
        mocked_create_request.assert_called_with('activities', params=params)
