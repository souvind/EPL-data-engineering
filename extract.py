# extract.py
# Prepare sample EPL dataset locally by copying from sample_data to data/raw.
import shutil
from pathlib import Path

SRC_DIR = Path("sample_data")
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def copy_samples():
    for f in SRC_DIR.glob("*.csv"):
        dest = RAW_DIR / f.name
        shutil.copy(f, dest)
        print(f"Copied {f} -> {dest}")

if __name__ == '__main__':
    copy_samples()
