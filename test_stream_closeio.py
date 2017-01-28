import stream_closeio as closeio
import unittest

class TestStreamCloseio(unittest.TestCase):
    def test_normalize_datetime(self):
        self.assertEqual(closeio.normalize_datetime(None), None)
        self.assertEqual(closeio.normalize_datetime('2015-01-01'), '2015-01-01T00:00:00+00:00')
        self.assertEqual(closeio.normalize_datetime('Mon, Dec 12, 2016 at 2:27 PM'), '2016-12-12T14:27:00+00:00')

if __name__ == '__main__':
    unittest.main()
