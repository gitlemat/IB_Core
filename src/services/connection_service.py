import time
import threading
from typing import TYPE_CHECKING
from config import Config

from .base_service import IBBaseService

if TYPE_CHECKING:
    from ib_connector import IBConnector

class IBConnectionService(IBBaseService):
    """
    Manages the core connection state and background threads for EClient.
    """
    def __init__(self, connector: 'IBConnector'):
        super().__init__(connector)
        self.monitor_thread = None
        self.executor_thread = None

    def start(self):
        """
        Starts the connection background thread.
        """
        self.logger.info("Starting IBConnectionService monitor...")
        self.monitor_thread = threading.Thread(target=self._monitor_connection, daemon=True)
        self.monitor_thread.start()

    def _monitor_connection(self):
        """
        Loop that checks potential disconnection and attempts reconnect.
        """
        while self.connector.started:
            if not self.connector.client.isConnected() and not self.connector.connected:
                try:
                    self.logger.info("Attempting connection to IB...")
                    self.connector.client.connect_to_server(Config.IB_HOST, Config.IB_PORT, Config.IB_CLIENT_ID)
                    
                    # If connect is successful, we need to run the loop
                    # BUT EClient.run() is blocking. We generally run it once.
                    # If we lost connection, EClient logic usually requires a clean restart of the loop
                    if self.executor_thread is None or not self.executor_thread.is_alive():
                        self.executor_thread = threading.Thread(target=self.connector.client.run, daemon=True)
                        self.executor_thread.start()
                        
                except Exception as e:
                    self.logger.error(f"Connection attempt failed: {e}")
            
            time.sleep(5) # Check every 5 seconds

    def stop(self):
        """
        Stops the connection.
        """
        if self.connector.connected:
            self.connector.client.disconnect_from_server()
