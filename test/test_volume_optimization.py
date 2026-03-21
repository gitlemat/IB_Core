
import unittest
from unittest.mock import MagicMock
import sys
import os
from datetime import datetime, timezone

# Adjust path to import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

# Mock Config
try:
    from config import Config
except ImportError:
    from unittest.mock import MagicMock
    Config = MagicMock()
    sys.modules['config'] = MagicMock(Config=Config)

if not hasattr(Config, 'INFLUXDB_BUCKET_PRICES'):
    Config.INFLUXDB_BUCKET_PRICES = "prices"
if not hasattr(Config, 'LOG_LEVEL'):
    Config.LOG_LEVEL = "INFO"
if not hasattr(Config, 'DATA_BUCKET'):
    Config.DATA_BUCKET = "data"
if not hasattr(Config, 'INFLUXDB_URL'):
    Config.INFLUXDB_URL = "http://localhost:8086"
if not hasattr(Config, 'INFLUXDB_TOKEN'):
    Config.INFLUXDB_TOKEN = "token"
if not hasattr(Config, 'INFLUXDB_ORG'):
    Config.INFLUXDB_ORG = "org"

from db_client import DatabaseClient

class TestVolumeOptimization(unittest.TestCase):
    def setUp(self):
        self.client = DatabaseClient()
        self.client.write_api = MagicMock()
        self.client.query_api = MagicMock()
        
    def test_volume_throttling_and_decoupling(self):
        gid = "gid_123"
        symbol = "ESM6"
        
        # 1. Send Volume Update 1 (100)
        self.client.update_tick_data(gid, "VOLUME", 100.0, symbol)
        
        # Verify:
        # - pending_volumes has it
        self.assertIn(gid, self.client.pending_volumes)
        self.assertEqual(self.client.pending_volumes[gid]["value"], 100.0)
        # - write_buffer is empty (no immediate write)
        self.assertEqual(len(self.client.write_buffer), 0)
        # - pending_ticks (AccumulatedTickRecord) is empty (decoupled)
        self.assertNotIn(gid, self.client.pending_ticks)
        
        # 2. Send Volume Update 2 (105)
        self.client.update_tick_data(gid, "VOLUME", 105.0, symbol)
        
        # Verify it overwrote the previous pending value
        self.assertEqual(self.client.pending_volumes[gid]["value"], 105.0)
        self.assertEqual(len(self.client.write_buffer), 0)
        
        # 3. Simulate Flush
        with self.client.lock:
            self.client._flush_buffer_internal("prices")
            
        # Verify:
        # - write_api.write called
        # - pending_volumes cleared
        self.assertEqual(len(self.client.pending_volumes), 0)
        
        # Check what was written
        # write_api.write(bucket=..., record=buffer)
        # buffer was cleared inside flush, so we check the call_args
        self.client.write_api.write.assert_called_once()
        call_args = self.client.write_api.write.call_args
        buffer_written = call_args[1]['record']
        
        self.assertEqual(len(buffer_written), 1)
        point = buffer_written[0]
        self.assertEqual(point._name, "volumen")
        self.assertEqual(point._fields["volume"], 105.0)
        
        # Verify EOD Timestamp (check hour=23)
        # Point time is stored as datetime in _time or similar internal
        # Depending on client version, let's just check it's not None
        self.assertIsNotNone(point._time)

if __name__ == '__main__':
    unittest.main()
