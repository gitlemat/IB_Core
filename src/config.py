import os
from dotenv import load_dotenv

# Load environment variables from .env file
def load_env_file():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Search in current dir and parent dir
    search_paths = [
        os.path.join(current_dir, ".env"),
        os.path.join(os.path.dirname(current_dir), ".env")
    ]
    for path in search_paths:
        if os.path.exists(path):
            load_dotenv(path)
            return path
    return None

_env_path = load_env_file()

class Config:
    """
    Application configuration class.
    Loads and provides easy access to environment variables.
    """
    
    # Project Root (IB_Core)
    #PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    #PROJECT_ROOT = sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    PROJECT_ROOT = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # App Mode
    APP_MODE = os.getenv("APP_MODE", "LAB").upper()
    
    # Interactive Brokers Settings
    IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
    
    # Auto-configure Port based on Mode
    if APP_MODE == "PROD":
        IB_PORT = int(os.getenv("IB_PORT", 4003))
    else:
        IB_PORT = int(os.getenv("IB_PORT", 4004))

    IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", 1))

    # InfluxDB Settings
    ENABLE_INFLUXDB = os.getenv("ENABLE_INFLUXDB", "TRUE").upper() == "TRUE"
    INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
    INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "")
    INFLUXDB_BUCKET_PRICES = os.getenv("INFLUXDB_BUCKET_PRICES", "ib_prices")
    
    # Internal usage: underlying bucket names
    _INFLUXDB_BUCKET_PROD = os.getenv("INFLUXDB_BUCKET_PROD", "ib_data_prod")
    _INFLUXDB_BUCKET_LAB = os.getenv("INFLUXDB_BUCKET_LAB", "ib_data_lab")
    
    # Derived Bucket for Executions/Account
    DATA_BUCKET = _INFLUXDB_BUCKET_PROD if APP_MODE == "PROD" else _INFLUXDB_BUCKET_LAB

    # App Settings
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    WATCHLIST_FILE = os.path.join(PROJECT_ROOT, os.getenv("WATCHLIST_FILE", "watchlist.json"))
    API_PORT = int(os.getenv("API_PORT", 8000))

    @classmethod
    def is_paper_trading(cls):
        """Returns True if running in LAB mode."""
        return cls.APP_MODE != "PROD"

