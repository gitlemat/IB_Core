import sys
import os
import logging

# Ensure we can import from ibapi
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ibapi")))

try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
except ImportError:
    # Dummy for development if ibapi not found
    class EClient:
        def __init__(self, wrapper): pass
    class EWrapper: pass

from logger import LoggerSetup

class IBClient(EClient):
    """
    Client class for sending requests to Interactive Brokers.
    Inherits from EClient.
    """
    
    def __init__(self, wrapper: EWrapper):
        """
        Initialize the IB Client.
        
        Args:
            wrapper (EWrapper): The wrapper instance that handles responses.
        """
        # EClient.__init__ takes the wrapper as an argument
        EClient.__init__(self, wrapper)
        self.logger = LoggerSetup.get_logger("IBClient")

    def connect_to_server(self, host: str, port: int, client_id: int):
        """
        Connects to the IB TWS/Gateway.
        
        Args:
            host (str): IP address (e.g., '127.0.0.1').
            port (int): Port number (e.g., 7497).
            client_id (int): Unique client ID.
        """
        self.logger.info(f"Connecting to IB Server at {host}:{port} with Client ID {client_id}...")
        self.connect(host, port, client_id)
        
    def disconnect_from_server(self):
        """
        Disconnects from the IB Server.
        """
        self.logger.info("Disconnecting from IB Server...")
        self.disconnect()
