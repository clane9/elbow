import tempfile
import time
from pathlib import Path

from elbow.loaders import load_parquet
from tests.utils_for_tests import extract_jsonl

DATASET_SIZE_MB = 17.8


dataset_path = Path(__file__).parent / "data"
source = str(dataset_path / "*.json")

with tempfile.TemporaryDirectory() as tmpdir:
    where = Path(tmpdir) / "fake_json.parquet"

    tic = time.monotonic()
    dset = load_parquet(
        source=source,
        extract=extract_jsonl,
        where=where,
    )
    runtime = time.monotonic() - tic

    tput = DATASET_SIZE_MB / runtime
    print(f"JSON benchmark throughput: {tput:.2f} MB/s")
