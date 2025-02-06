import logging
import os
import time

from dotenv import find_dotenv, load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logging.Formatter.converter = time.gmtime

logger = logging.getLogger(__name__)

APP_ENV = os.environ.get("APP_ENV", "local")
logger.info(f"Loading {APP_ENV} environment variables")
load_dotenv(find_dotenv(f".env.{APP_ENV}"))
