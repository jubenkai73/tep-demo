import shutil
import kagglehub
import yaml
from pathlib import Path
from src.config import RAW_DATA_PATH, CACHE_CONFIG_PATH


class DataProcessor:
    def download_csv(self, dataset_name="afrniomelo/tep-csv"):
        """Downloads a CSV from Kaggle and tracks cache information"""

        if RAW_DATA_PATH.exists():
            print(f"✔️ Data already exists in {RAW_DATA_PATH}")
            return str(RAW_DATA_PATH)

        print("✔️ Downloading dataset from Kaggle")
        temp_download_path = kagglehub.dataset_download(dataset_name)
        print(f"Temporary download path: {temp_download_path}")

        RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(temp_download_path, RAW_DATA_PATH, dirs_exist_ok=True)

        self._save_cache_info(temp_download_path)

        print(f"✔️ Download completed successfully")
        return str(RAW_DATA_PATH)

    def _save_cache_info(self, cache_path):
        """Saves the Kaggle cache location"""
        cache_info = {
            "kaggle_cache_path": str(cache_path),
            "data_path": str(RAW_DATA_PATH),
        }

        CACHE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_CONFIG_PATH, "w") as f:
            yaml.dump(cache_info, f, default_flow_style=False)

        print(f"✔️ Cache info saved to {CACHE_CONFIG_PATH}")


class DataPreprocessor:
    pass
