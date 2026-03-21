import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

try:
    print("Attempting to import app modules...")
    from IB_Core import config
    from IB_Core import logger
    from IB_Core import models
    from IB_Core import utils
    from IB_Core import watchlist
    from IB_Core import db_client
    from IB_Core import ib_wrapper
    from IB_Core import ib_client
    from IB_Core import ib_connector
    from IB_Core import api
    from IB_Core import main
    print("SUCCESS: All modules imported correctly.")
except ImportError as e:
    print(f"ERROR: Import failed: {e}")
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Unexpected error: {e}")
    sys.exit(1)
