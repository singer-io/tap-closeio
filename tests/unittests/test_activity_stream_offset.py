from tap_closeio.schemas import IDS
import unittest
from tap_closeio.context import Context
from tap_closeio.streams import paginated_sync, create_request

class TestExistingStateOffset(unittest.TestCase):
    def test_existing_state_existing_state_offset_greater_than_250K(self):
        config = {
            "start_date": "2022-01-01",
            "api_key": "test_API_key"
        }
        state = {
            "currently_syncing": "activities",
            "bookmarks": {
                "activities": {
                    "date_created": "2022-04-01T00:00:00",
                    "offset": {"skip": 259000}
                }
            }
        }
        context = Context(config, state)
        offset = context.get_offset(["activities", "skip"])
        self.assertEqual(offset, 0)

    def test_existing_state_existing_state_offset_lesser_than_250K(self):
        config = {
            "start_date": "2022-01-01",
            "api_key": "test_API_key"
        }
        state = {
            "currently_syncing": "activities",
            "bookmarks": {
                "activities": {
                    "date_created": "2022-04-01T00:00:00",
                    "offset": {"skip": 1000}
                }
            }
        }
        context = Context(config, state)
        offset = context.get_offset(["activities", "skip"])
        self.assertEqual(offset, 1000)
