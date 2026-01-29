import pandas as pd
from pathlib import Path
from src.training.loader import DataLoader
from src.config import (
    RAW_DATA_PATH,
    RAW_PARQUET_DIR,
    OPTIMIZED_DTYPES,
    RAW_CSV_FILES,
    MERGED_FILE_PATH,
    FAULTY_TRAIN_FILENAME,
    NORMAL_TRAIN_FILENAME
)


class DataProcessor:
    """Orchestrates data transformation and consolidation within the Silver Layer.

    This class manages the lifecycle of raw-to-optimized data conversion and
    merges distributed datasets into a unified master record for the training pipeline.
    """

    def __init__(self) -> None:
        """Initializes the DataProcessor with internal dependencies.

        The internal DataLoader instance is utilized for specialized data handling
        methods if required during the processing lifecycle.
        """
        self.loader: DataLoader = DataLoader()

    def convert_csv_to_parquet(self) -> None:
        """Transmutes raw CSV artifacts into memory-efficient Parquet format.

        This method enforces idempotency by validating the existence of source
        files and skipping already processed artifacts. It applies optimized
        schema dtypes during ingestion to minimize RAM footprint.

        Returns:
            None: Logs operational status and conversion metrics to stdout.
        """
        # Enforce filesystem integrity for the Silver Layer
        RAW_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

        converted_count: int = 0

        # Iterative ingestion of defined raw artifacts
        for csv_name in RAW_CSV_FILES:
            csv_path: Path = RAW_DATA_PATH / csv_name
            parquet_name: str = csv_name.replace(".csv", ".parquet")
            parquet_path: Path = RAW_PARQUET_DIR / parquet_name

            # Pre-flight check: Source existence
            if not csv_path.exists():
                print(f"⚠️ Source artifact missing: {csv_name}")
                continue

            # Idempotency check: Skip existing optimized files
            if parquet_path.exists():
                print(f"⏩ Already optimized: {parquet_name}")
                continue

            # Atomic conversion process
            try:
                # Optimized read with pre-defined schema dtypes
                df: pd.DataFrame = pd.read_csv(csv_path, dtype=OPTIMIZED_DTYPES)
                df.to_parquet(parquet_path, engine="pyarrow", index=False)
                print(f"✅ Optimized: {csv_name} → {parquet_name}")
                converted_count += 1
            except Exception as e:
                print(f"❌ Transformation failure for {csv_name}: {e}")

        if converted_count == 0:
            print("✅ Silver Layer is fully synchronized")
        else:
            print(f"🏁 Processing cycle complete: {converted_count} artifact(s) generated.")

    def merge_faulty_and_normal_data(self) -> pd.DataFrame:
        """Consolidates discrete training sets into a unified Silver Master record.

        Resolves class imbalances by labeling normal data and ensures data
        persistence in a high-performance Parquet format.

        Returns:
            pd.DataFrame: The consolidated dataset containing both normal and faulty records.

        Raises:
            FileNotFoundError: If upstream Parquet dependencies are missing.
        """
        # Guard clause: Return existing artifact to avoid redundant compute
        if MERGED_FILE_PATH.exists():
            print(f"✅ Master record detected: {MERGED_FILE_PATH.name}")
            return pd.read_parquet(MERGED_FILE_PATH)

        faulty_path = RAW_PARQUET_DIR / FAULTY_TRAIN_FILENAME
        normal_path = RAW_PARQUET_DIR / NORMAL_TRAIN_FILENAME

        # Loading upstream artifacts via PyArrow engine
        print(f"📖 Ingesting: {faulty_path.name}")
        faulty_df: pd.DataFrame = pd.read_parquet(faulty_path)

        print(f"📖 Ingesting: {normal_path.name}")
        normal_df: pd.DataFrame = pd.read_parquet(normal_path)

        if normal_df.empty and faulty_df.empty:
            print("❌ Critical Error: Source dataframes are empty. Aborting merge.")
            return pd.DataFrame()

        # Data Harmonization: Assigning baseline class (0) to normal observations
        print("🔗 Vertical concatenation in progress...")
        if "faultNumber" not in normal_df.columns:
            normal_df["faultNumber"] = 0

        # Master dataset generation
        merged_df: pd.DataFrame = pd.concat([normal_df, faulty_df], axis=0, ignore_index=True)

        # Persistent storage for downstream Gold Layer processing
        merged_df.to_parquet(MERGED_FILE_PATH, engine="pyarrow", index=False)
        print(f"✅ Consolidated record saved: {MERGED_FILE_PATH.name}")

        return merged_df
