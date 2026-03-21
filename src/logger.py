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
        Creates and configures a logger instance.
        Logs are saved to logs/IB_Core.log.
        Daily rotation moves old logs to logs/oldlogs/IB_Core_YYYYMMDD.log.
        """
        import os
        from logging.handlers import TimedRotatingFileHandler
        
        logger = logging.getLogger(name)
        
        # Set the log level based on configuration
        level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
        logger.setLevel(level)
        
        # Ensure handlers are set up only once (to avoid duplicate logs if get_logger called multiple times)
        if not logger.handlers:
            # 1. Define Paths
            base_log_dir = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs")))
            old_log_dir = os.path.join(base_log_dir, "oldlogs")
            os.makedirs(old_log_dir, exist_ok=True)
            
            log_file = os.path.join(base_log_dir, "IB_Core.log")
            
            # 2. Config Handler: TimedRotating
            # when='midnight': rotate at midnight
            # interval=1: every 1 day
            handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1)
            handler.suffix = "%Y%m%d" # Suffix used by Handler to tag old files: IB_Core.log.20260118
            handler.setLevel(level)
            
            # Format
            formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            
            
            logger.addHandler(handler)
            
            # 3. Custom Namer: logic to determine the DESTINATION filename for archived log
            def custom_namer(default_name):
                # default_name comes as "logs/IB_Core.log.20260118"
                # We want "logs/oldlogs/IB_Core_20260118.log"
                
                # Split path to get filename
                path, filename = os.path.split(default_name) 
                # path="logs", filename="IB_Core.log.20260118"
                
                # Split parts. Expecting: IB_Core, log, YYYYMMDD
                parts = filename.split('.')
                # If name is exactly IB_Core.log.YYYYMMDD -> ['IB_Core', 'log', 'YYYYMMDD']
                
                if len(parts) >= 3:
                    date_part = parts[-1]
                    # Construct new name
                    new_filename = f"IB_Core_{date_part}.log"
                    return os.path.join(path, "oldlogs", new_filename)
                
                return default_name

            # 4. Custom Rotator: Logic to actally move the file
            def custom_rotator(source, dest):
                # source: logs/IB_Core.log.20260118
                # dest: logs/oldlogs/IB_Core_20260118.log (result of namer)
                if os.path.exists(source):
                    os.rename(source, dest)

            handler.namer = custom_namer
            handler.rotator = custom_rotator

            # 5. Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            
            # Add handler to logger
            logger.addHandler(handler)
            
            # Remove default console handlers if any exist (from root logger usually, but good practice to be clean)
            logger.propagate = False # Stop propagation to root logger to avoid console spam if root has console handler
            
        return logger
