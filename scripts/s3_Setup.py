from pathlib import Path
import shutil

PROJECT_ROOT = Path(__file__).resolve().parents[1]

source_to_target = {
    "data/raw/branch_master": "s3/raw/branch_master",
    "data/raw/customers": "s3/raw/customers",
    "data/raw/loan_applications": "s3/raw/loan_applications",
    "data/raw/loan_accounts": "s3/raw/loan_accounts",
    "data/raw/emi_payments": "s3/raw/emi_payments",
    "data/raw/delinquency": "s3/raw/delinquency",
}

for source, target in source_to_target.items():
    source_path = PROJECT_ROOT / source
    target_path = PROJECT_ROOT / target

    target_path.mkdir(parents=True, exist_ok=True)

    if not source_path.exists():
        print(f"Missing source folder: {source_path}")
        continue

    for file_path in source_path.glob("*.csv"):
        destination = target_path / file_path.name
        shutil.copy2(file_path, destination)
        print(f"Copied: {file_path} -> {destination}")

print("S3 setup completed.")
