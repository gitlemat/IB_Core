import sys
import os
import json
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from db_client import DatabaseClient
from config import Config
from influxdb_client import InfluxDBClient

db = DatabaseClient()
db.query_api = InfluxDBClient(url=Config.INFLUXDB_URL, token=Config.INFLUXDB_TOKEN, org=Config.INFLUXDB_ORG).query_api()

states = db.get_last_order_states([82, 97])
print(states)
