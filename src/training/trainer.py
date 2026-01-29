import joblib
import pandas as pd
from pathlib import Path
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from src.config import (
    MODEL_DIR,
    MODEL_DETECT_NAME,
    MODEL_DIAG_NAME,
    DETECTOR_PARAMS,
    DIAGNOSTICIAN_PARAMS
)

class ModelTrainer:
    """Orchestrates the training of a cascaded model architecture.

    This class handles a two-stage classification pipeline:
    1. Binary Detection (Normal vs. Anomaly).
    2. Multiclass Diagnosis (Fault identification).
    It implements idempotency features to skip training if artifacts already exist.
    """

    def __init__(self) -> None:
        """Initializes the trainer and ensures the model repository exists.

        The initialization ensures the MODEL_DIR exists on the filesystem
        to prevent downstream FileNotFoundError during serialization.
        """
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        self.detector_path: Path = MODEL_DIR / MODEL_DETECT_NAME
        self.diagnostician_path: Path = MODEL_DIR / MODEL_DIAG_NAME
        self.detector: Pipeline | None = None
        self.diagnostician: Pipeline | None = None

    def train_cascaded_models(self, df_train: pd.DataFrame, force: bool = False) -> None:
        """Executes the hierarchical training lifecycle for the cascade architecture.

        Args:
            df_train (pd.DataFrame): Training dataset containing features and 'faultNumber'.
            force (bool): If True, overrides existing artifacts and re-runs training.
                Defaults to False.

        Returns:
            None: Artifacts are persisted directly to the filesystem.
        """
        # --- Idempotency Check ---
        # Short-circuit the execution if models are already present to save compute resources
        if not force and self.detector_path.exists() and self.diagnostician_path.exists():
            print(f"\n⏭️  MODELS EXIST: Found in {MODEL_DIR}")
            print("⏭️  Skipping training phase to optimize pipeline execution")
            print("💡 Pass 'force=True' to override existing artifacts")
            return

        # ==========================================
        # 🛡️ PHASE 1 : THE DETECTOR (Binary)
        # ==========================================
        if force or not self.detector_path.exists():
            print("\n▶ PHASE 1: Training DETECTOR (Binary Anomaly Detection)")

            # Binary label mapping: 0 = Normal, 1 = Any Fault type
            y_train_binary: pd.Series = (df_train['faultNumber'] > 0).astype(int)
            X_train: pd.DataFrame = self._prepare_features(df_train)

            self.detector = Pipeline([
                ('scaler', StandardScaler()),
                ('classifier', RandomForestClassifier(**DETECTOR_PARAMS))
            ])

            self.detector.fit(X_train, y_train_binary)
            self._save_model(self.detector, MODEL_DETECT_NAME)
        else:
            print(f"✅ DETECTOR: Artifact already exists at {MODEL_DETECT_NAME}.")

        # ==========================================
        # 🕵️ PHASE 2 : THE DIAGNOSTICIAN (Multiclass)
        # ==========================================
        if force or not self.diagnostician_path.exists():
            print("\n▶ PHASE 2: Training DIAGNOSTICIAN (Fault Classification)")

            # Filter training set: Diagnostician only learns from faulty patterns
            mask_faulty: pd.Series = df_train['faultNumber'] > 0
            df_train_faulty: pd.DataFrame = df_train[mask_faulty]

            X_train_diag: pd.DataFrame = self._prepare_features(df_train_faulty)
            y_train_diag: pd.Series = df_train_faulty['faultNumber']

            self.diagnostician = Pipeline([
                ('scaler', StandardScaler()),
                ('classifier', RandomForestClassifier(**DIAGNOSTICIAN_PARAMS))
            ])

            self.diagnostician.fit(X_train_diag, y_train_diag)
            self._save_model(self.diagnostician, MODEL_DIAG_NAME)
        else:
            print(f"✅ DIAGNOSTICIAN: Artifact already exists at {MODEL_DIAG_NAME}.")

    def _prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prunes metadata and target columns to isolate model features.

        Args:
            df (pd.DataFrame): Input dataframe with raw sensors and metadata.

        Returns:
            pd.DataFrame: Cleaned dataframe containing only training features.
        """
        metadata: list[str] = ['faultNumber', 'simulationRun', 'sample', 'unique_run_id']
        cols_to_drop: list[str] = [c for c in metadata if c in df.columns]
        return df.drop(columns=cols_to_drop)

    def _save_model(self, model: Pipeline, filename: str) -> None:
        """Serializes the trained scikit-learn pipeline to disk.

        Args:
            model (Pipeline): The fitted estimator/pipeline to persist.
            filename (str): Target filename within the MODEL_DIR.

        Returns:
            None
        """
        save_path: Path = MODEL_DIR / filename
        joblib.dump(model, save_path)
        print(f"📦 Artifact persisted: {save_path.name}")
