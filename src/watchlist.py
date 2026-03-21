import json
import os
from typing import List, Dict
from logger import LoggerSetup
from config import Config
from models import IBContract

class WatchlistManager:
    """
    Manages the persistence of the Watchlist (list of symbols to track).
    Saves/Loads from a JSON file.
    """
    
    def __init__(self, filepath: str = Config.WATCHLIST_FILE):
        self.logger = LoggerSetup.get_logger("WatchlistManager")
        self.filepath = filepath
        self.symbols: List[str] = []
        self.load()

    def load(self):
        """Loads the watchlist from the JSON file."""
        if not os.path.exists(self.filepath):
            self.logger.info(f"No existing watchlist file found at {self.filepath}. Starting empty.")
            self.symbols = []
            return

        try:
            with open(self.filepath, 'r') as f:
                self.symbols = json.load(f)
            self.logger.info(f"Loaded {len(self.symbols)} symbols from watchlist.")
        except Exception as e:
            self.logger.error(f"Failed to load watchlist: {e}")
            self.symbols = []

    def save(self):
        """Saves any changes to the watchlist file."""
        try:
            with open(self.filepath, 'w') as f:
                json.dump(self.symbols, f, indent=4)
            self.logger.info(f"Saved {len(self.symbols)} symbols to watchlist.")
        except Exception as e:
            self.logger.error(f"Failed to save watchlist: {e}")

    def add_contract(self, symbol: str) -> bool:
        """
        Adds a symbol to the watchlist if it doesn't already exist.
        """
        if symbol in self.symbols:
            self.logger.info(f"Symbol {symbol} already in watchlist.")
            return False
            
        self.symbols.append(symbol)
        self.save()
        return True

    def remove_contract(self, symbol: str) -> bool:
        """
        Removes a symbol from the watchlist.
        """
        if symbol in self.symbols:
            self.symbols.remove(symbol)
            self.save()
            return True
        return False

    def get_contracts(self) -> List[str]:
        """Returns the list of symbols."""
        return self.symbols
