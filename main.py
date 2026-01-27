import sys
<<<<<<< HEAD
from pathlib import Path

from src.preprocessing.downloader import DataDownloader
from src.preprocessing.processor import DataProcessor


class MLPipeline:
    """Orchestrates the ML pipeline."""

    def __init__(self):
        """Initializes the components of the pipeline."""
        self.downloader = DataDownloader()
        self.processor = DataProcessor()

    def preprocess(self) -> int:
        """Orchestrates the preprocessing steps of the ML pipeline.

        Returns:
            int: Exit status code (0 for success).

        Raises:
            Exception: If an error occurs during preprocessing.
        """
        print("\n" + "="*70)
        print("🚀 PIPELINE TEP FAULT DETECTION")
        print("="*70)

        # --- Step 1 ---
        print("\n▶ STEP 1: Download")
        self.downloader.download()

        # --- Step 2 ---
        print("\n▶ STEP 2: Convert CSV to Parquet")
        self.processor.convert_csv_to_parquet()

        # --- Step 3 ---
        print("\n▶ STEP 3: Merge Datasets")
        self.processor.merge_faulty_and_normal_data()

        print("\n" + "="*70)
        print("✅ PIPELINE COMPLETED")
        print("="*70 + "\n")

        return 0


def main():
    """Main function to orchestrate the execution of the pipeline."""
    # Initialize pipeline
    pipeline = MLPipeline()

    try:
        # Execute preprocessing steps
        pipeline.preprocess()
        return 0
    except Exception as e:
        print(f"Error during pipeline execution: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
=======
import argparse
#from pathlib import Path

from src.preprocessing.downloader import DataDownloader
from src.preprocessing.converter import DataConverter



class MLPipeline:
    """Orchestrateur du pipeline ML"""

    def __init__(self):
        """Initialise les composants du pipeline"""
        self.downloader = DataDownloader()
        self.converter = DataConverter()

    def preprocess(self) -> int:

            # Step 1: Download
            print("Step 1/2: Downloading data from Kaggle")
            self.downloader.download()

            # Step 2: Convert
            print("Step 2/2: Converting CSV to Parquet")
            self.converter.convert_csv_to_parquet()

            print("✔️ Preprocessing completed successfully")
            return 0

def main():
    # Initialize pipeline
    pipeline = MLPipeline()

    # Execute preprocessing steps
    pipeline.preprocess()

# Assurez-vous d'ajouter cette ligne pour exécuter le script principal
if __name__ == "__main__":
    main()
>>>>>>> origin/main
