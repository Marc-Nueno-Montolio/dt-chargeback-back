import os
import logging
import logging
from logging.handlers import RotatingFileHandler
# Logging Settings
LOG_FORMAT= "%(asctime)s - %(levelname)s - %(message)s"
LOG_LEVEL = logging.DEBUG



# Set up logging to file
log_handler = RotatingFileHandler('chargeback.log', backupCount=5)
log_handler.setFormatter(logging.Formatter(LOG_FORMAT))
log_handler.setLevel(LOG_LEVEL)

# Get root logger and add file handler
root_logger = logging.getLogger()
root_logger.addHandler(log_handler)
root_logger.setLevel(LOG_LEVEL)

# Dynatrace API Settings
BASE_URL = "https://apmactivegate.tech.ec.europa.eu/e/39a3e95b-5423-482c-879b-99ef235dffeb"
DT_TOKEN = os.getenv("DT_TOKEN")
USER_AGENT = "ec-dps-chargeback-1.0.0"

# Multiple Worker Threads Settings
TOPOLOGY_REFRESH_THREADS = 4
DT_QUERIES_THREADS = 30

# Database Settings
SQLALCHEMY_DATABASE_URL = "sqlite:///./chargeback.db"
CHECK_SAME_THREAD = False


# Input data paths settings
MANAGED_HOST_TAGS_INPUT_FILE = "./input/managed_host_tags.txt"
MANAGED_IS_NAMES_INPUT_FILE = "./input/managed_is_names.txt"