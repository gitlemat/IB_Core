from typing import TYPE_CHECKING
from logger import LoggerSetup

if TYPE_CHECKING:
    from main_facade import IBConnector  # Will be the refactored IBConnector

class IBBaseService:
    """
    Base class for all IB Connector services.
    Provides access to the main connector facade and its logger.
    """
    def __init__(self, connector: 'IBConnector'):
        self.connector = connector
        # Use child class name for logger via our common utility
        self.logger = LoggerSetup.get_logger(self.__class__.__name__)
