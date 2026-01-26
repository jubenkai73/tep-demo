from src.preprocessor import DataProcessor
from src.config import RAW_DATA_PATH

def main():
    processor = DataProcessor()

    print("=== Download Test ===")
    result = processor.download_csv()
    print(f"Result: {result}")

    print(f"\n=== Verification ===")
    print(f"Data exists: {RAW_DATA_PATH.exists()}")
    if RAW_DATA_PATH.exists():
        files = list(RAW_DATA_PATH.glob("*.csv"))
        print(f"CSV files found: {len(files)}")
        for f in files[:3]:  # Display first 3 files
            print(f"  - {f.name}")

if __name__ == "__main__":
    main()
