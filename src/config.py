from pathlib import Path
import os

# === Base Directories ===
BASE_DIR = Path(__file__).resolve().parent.parent  # Navigate to the project root
DATA_DIR = Path(os.getenv("DATA_PATH", BASE_DIR / "data"))

# === Data Directories ===
RAW_DATA_PATH = DATA_DIR / "raw" / "tep-csv"
PROCESSED_DATA_PATH = DATA_DIR / "processed"

# === Configuration ===
CONFIG_DIR = BASE_DIR / "config"
CACHE_CONFIG_PATH = CONFIG_DIR / "cache.yaml"

# === Other Directories ===
LOGS_DIR = BASE_DIR / "logs"
MODELS_DIR = BASE_DIR / "models"
