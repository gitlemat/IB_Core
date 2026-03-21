from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import sys
import os

# Allow running directly with 'python3 IB_Core/src/main.py' 
# We don't necessarily need to add project root if importing relatively, but adding 'src' to path is default.
# The original code added '..' (IB_Core) to path.
# Now we are IN src.
# If we run 'python src/main.py', sys.path[0] is src.
# So 'import utils' works.
# No need to append anything unless we need IB_Core root modules (none exist).

from ib_connector import IBConnector
from api import router
from logger import LoggerSetup
from config import Config

# Setup Logger
logger = LoggerSetup.get_logger("Main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup
    logger.info("Initializing IB Connector...")
    ib_connector = IBConnector()
    ib_connector.start()
    
    # Store in app state so routes can access it
    app.state.ib_connector = ib_connector
    
    yield
    
    # Shutdown
    logger.info("Stopping IB Connector...")
    ib_connector.stop()

app = FastAPI(title="IB Management API", lifespan=lifespan)

# Include Routes
app.include_router(router)

@app.get("/")
def read_root():
    return {"status": "IB Management App Running"}

if __name__ == "__main__":
    # For local running
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=Config.API_PORT, reload=True)
    except KeyboardInterrupt:
        print("\nIB Management API stopped by user.")
