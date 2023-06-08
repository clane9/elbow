import tempfile
import time
from pathlib import Path

import pandas as pd

from elbow.builders import build_parquet
from tests.utils_for_tests import extract_jsonl

# TODO: do I need to hardcode this?
DATASET_SIZE_MB = 17.8


dataset_path = Path(__file__).parent / "data"
source = str(dataset_path / "*.json")

with tempfile.TemporaryDirectory() as tmpdir:
    where = Path(tmpdir) / "fake_json.pqds"

    tic = time.monotonic()
    build_parquet(
        source=source,
        extract=extract_jsonl,
        where=where,
    )
    runtime = time.monotonic() - tic

    df = pd.read_parquet(where)

    tput = DATASET_SIZE_MB / runtime
    print(f"JSON benchmark:  rows written: {len(df)}, throughput: {tput:.2f} MB/s")
