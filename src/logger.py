import logging
import sys
from config import Config

class LoggerSetup:
    """
    Utility class to configure the application logger.
    """
    
    @staticmethod
    def get_logger(name: str):
        """
        Creates and configures a logger instance within the IB_Core namespace.
        All loggers share a single TimedRotatingFileHandler attached to the parent 'IB_Core'.
        """
        import os
        from logging.handlers import TimedRotatingFileHandler

        # 1. Ensure hierarchical name (IB_Core.Name)
        root_name = "IB_Core"
        if name == root_name:
            full_name = root_name
        elif not name.startswith(f"{root_name}."):
            full_name = f"{root_name}.{name}"
        else:
            full_name = name
            
        logger = logging.getLogger(full_name)
        
        # 2. Set the log level
        level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
        logger.setLevel(level)
        
        # 3. Initialize Shared Parent Handler if needed
        root_logger = logging.getLogger(root_name)
        if not root_logger.handlers:
            # Define Paths
            base_log_dir = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs")))
            old_log_dir = os.path.join(base_log_dir, "oldlogs")
            os.makedirs(old_log_dir, exist_ok=True)
            log_file = os.path.join(base_log_dir, "IB_Core.log")
            
            # Config Handler: TimedRotating
            handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1)
            handler.suffix = "%Y%m%d"
            handler.setLevel(level)
            
            # Format: Include logger name to distinguish services
            formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            
            # Custom naming and rotation logic
            def custom_namer(default_name):
                path, filename = os.path.split(default_name) 
                parts = filename.split('.')
                if len(parts) >= 3:
                    date_part = parts[-1]
                    new_filename = f"IB_Core_{date_part}.log"
                    return os.path.join(path, "oldlogs", new_filename)
                return default_name

            def custom_rotator(source, dest):
                if os.path.exists(source):
                    os.rename(source, dest)

            handler.namer = custom_namer
            handler.rotator = custom_rotator
            
            root_logger.addHandler(handler)
            # Root should not propagate to standard logging root to avoid console duplication
            root_logger.propagate = False 

        return logger
