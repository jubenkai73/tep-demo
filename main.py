import sys
import pandas as pd
from src.preprocessing.downloader import DataDownloader
from src.preprocessing.processor import DataProcessor
from src.training.loader import DataLoader
from src.training.trainer import ModelTrainer

class MLPipeline:
    """Orchestrates the ML pipeline."""

    def __init__(self) -> None:
        self.downloader = DataDownloader()
        self.processor = DataProcessor()
        self.loader = DataLoader()
        self.trainer = ModelTrainer()

    def preprocess(self) -> int:
        """Exécute la partie ETL (Extract, Transform, Load)."""
        print("\n" + "="*70)
        print("🚀 STARTING PREPROCESSING")
        print("="*70)

        print("\n▶ STEP 1: Download CSV TEP")
        self.downloader.download()

        print("\n▶ STEP 2: Convert CSV to Parquet")
        self.processor.convert_csv_to_parquet()

        print("\n▶ STEP 3: Merge Datasets")
        self.processor.merge_faulty_and_normal_data()

        print("\n✅ PREPROCESSING COMPLETED")
        return 0

    def train(self) -> int:
        """Phase de chargement, split et préparation à l'entraînement."""
        print("\n" + "="*70)
        print("🧠 STARTING DATA PREPARATION & TRAINING")
        print("="*70)

        # --- Step 1 : Chargement intelligent ---
        print("\n▶ STEP 1: Loading Data (with cache check)")
        df = self.loader.load_data()

        # --- Step 2 : Split Train/Test par Run ---
        print("▶ STEP 2: Splitting Data by Run (Avoid Leakage)")
        (X_train, y_train), (X_test, y_test) = self.loader.split_by_run(df)

        print("✅ Data split completed:")
        print(f"   - Train set: {X_train.shape[0]} samples")
        print(f"   - Test set:  {X_test.shape[0]} samples")
        print(f"📊 Features: {X_train.shape[1]} sensors")

        # --- Step 3 : Sauvegarde du Test Set ---
        print("\n▶ STEP 3: Archiving Test Set")
        self.loader.save_test_set(X_test, y_test)

        # --- Step 4 : Training  ---
        print("\n▶ STEP 4: Model Training (Cascaded Models)")

        # Reconstruction du DataFrame pour le trainer (qui gère ses propres filtres)
        df_train_ready = pd.concat([X_train, y_train], axis=1)
        self.trainer.train_cascaded_models(df_train_ready, force=False)

        print("\n" + "="*70)
        print("✅ PIPELINE COMPLETED")
        print("="*70)
        return 0

def main() -> int:
    pipeline = MLPipeline()
    try:
        pipeline.preprocess()
        pipeline.train()
        return 0
    except Exception as e:
        print(f"\n❌ Error during pipeline execution: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
