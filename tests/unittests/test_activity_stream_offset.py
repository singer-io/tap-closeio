from unittest import mock
from tap_closeio.schemas import IDS
from tap_closeio.context import Context
from tap_closeio.streams import paginated_sync, create_request
import unittest

# mock Page class and set next_skip and records in the page
class MockPage:
    def __init__(self, records, next_skip):
        self.records = records
        self.next_skip = next_skip

# mock paginate function and yield MockPage
def mock_paginate(*args, **kwargs):
    pages = [
        MockPage([{"id": 1, "date_created": "2022-01-02"}, {"id": 1, "date_created": "2022-01-03"}], 2),
        MockPage([{"id": 1, "date_created": "2022-01-04"}, {"id": 1, "date_created": "2022-01-04"}], 4)
    ]
    # yield 1st page, as a result after 1st page, the skip will be 2
    yield pages[0]
    # if there is param in the request to raise error, then raise max_skip error
    if args[2].params.get("error"):
        raise Exception("The skip you set is larger than the maximum skip for this resource (max_skip = 250000).")
    # yield 2nd page
    yield pages[1]

# mock format_dts function and return 3rd argument, ie. record
def mock_format_dts(*args, **kwargs):
    return args[2]

class TestExistingStateOffset(unittest.TestCase):
    """
        Test cases to verify we are returning 0 if skip > 250K else return the skip in the existing state file
    """
    def test_existing_state_offset_greater_than_250K(self):
        """
            Test case to verify we are returning 0 as skip is greater than 250K in existing state
        """
        # mock config
        config = {
            "start_date": "2022-01-01",
            "api_key": "test_API_key"
        }
        # mock state with skip > 250K
        state = {
            "currently_syncing": "activities",
            "bookmarks": {
                "activities": {
                    "date_created": "2022-04-01T00:00:00",
                    "offset": {"skip": 259000}
                }
            }
        }
        # create Context with config and state
        context = Context(config, state)
        # function call
        offset = context.get_offset(["activities", "skip"])
        # verify we got 0 as we had skip > 250K
        self.assertEqual(offset, 0)

    def test_existing_state_offset_lesser_than_250K(self):
        """
            Test case to verify we are returning existing skip as it is lesser than 250K in existing state
        """
        # mock config
        config = {
            "start_date": "2022-01-01",
            "api_key": "test_API_key"
        }
        # mock state with skip < 250K
        state = {
            "currently_syncing": "activities",
            "bookmarks": {
                "activities": {
                    "date_created": "2022-04-01T00:00:00",
                    "offset": {"skip": 1000}
                }
            }
        }
        # create Context with config and state
        context = Context(config, state)
        # function call
        offset = context.get_offset(["activities", "skip"])
        # verify we got existing skip from the state as skip < 250K
        self.assertEqual(offset, 1000)

@mock.patch("tap_closeio.streams.write_records")
@mock.patch("tap_closeio.streams.LOGGER.warning")
@mock.patch("tap_closeio.streams.paginate", side_effect = mock_paginate)
@mock.patch("tap_closeio.streams.format_dts", side_effect = mock_format_dts)
class TestOffsetClear(unittest.TestCase):
    def test_clear_state_for_max_skip_error(self, mocked_format_dts, mocked_paginate, mocked_logger_warning, mocked_write_records):
        """
            Test case to verify we clear the 'skip' when we encounter 'max_skip' error for 'activity' stream
        """
        # mock config
        config = {
            "start_date": "2022-01-01",
            "api_key": "test_API_key"
        }
        # mock state
        state = {}
        # mock param to raise 'max_skip' error
        params = {"error": "true"}
        # create Context with config and state
        context = Context(config, state)
        # create request for activity stream with params
        request = create_request(IDS.ACTIVITIES, params)

        # verify we raise Exception during function call
        with self.assertRaises(Exception) as e:
            paginated_sync(IDS.ACTIVITIES, context, request, "2022-01-01")

        # verify the error message
        self.assertEqual(str(e.exception), "The skip you set is larger than the maximum skip for this resource (max_skip = 250000). So, clearing skip offset, please reduce the date window size and try again.")
        # verify we did not get 'skip' in the state file
        self.assertIsNone(state.get("bookmarks").get("activities").get("offset").get("skip"))
        # verify we log 'reduce date window' message
        mocked_logger_warning.assert_called_with("Hit max_skip error so clearing skip offset, please reduce the date window size and try again.")
