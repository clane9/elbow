import json
import string
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pytest

from elbow import as_record, load_parquet, load_table
from elbow.extractors import file_meta
from elbow.typing import StrOrPath

NUM_BATCHES = 64
BATCH_SIZE = 256

# TODO: may want to benchmark these with pytest-benchmark


@pytest.fixture(scope="module")
def mod_tmp_path(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("tmp")


@pytest.fixture(scope="module")
def jsonl_dataset(mod_tmp_path: Path) -> str:
    for ii in range(NUM_BATCHES):
        _random_jsonl_batch(ii, mod_tmp_path, BATCH_SIZE)

    pattern = str(mod_tmp_path / "*.json")
    return pattern


def _random_jsonl_batch(idx: int, tmp_path: Path, batch_size: int):
    rng = np.random.default_rng(2022 + idx)
    path = tmp_path / f"{_random_string(rng, 8)}.json"

    with path.open("w") as f:
        for _ in range(batch_size):
            rec = _random_record(rng)
            print(json.dumps(rec), file=f)
    return path


def _random_record(rng: np.random.Generator) -> Dict[str, Any]:
    rec = {
        "a": int(rng.integers(0, 10)),
        "b": float(rng.random()),
        "c": _random_string(rng, 32),
        "d": rng.normal(size=rng.integers(0, 100)).tolist(),
    }
    return rec


def _random_string(rng: np.random.Generator, length: int):
    return "".join(rng.choice(list(string.ascii_letters), length))


def extract_jsonl(path: StrOrPath):
    metadata = as_record(file_meta(path))

    with open(path) as f:
        for line in f:
            record = json.loads(line)
            # with metadata
            record = metadata + record
            yield record


def test_load_table(jsonl_dataset: str):
    df = load_table(pattern=jsonl_dataset, extract=extract_jsonl)
    assert df.shape == (NUM_BATCHES * BATCH_SIZE, 7)

    expected_columns = ["file_path", "link_target", "mod_time", "a", "b", "c", "d"]
    assert df.columns.tolist() == expected_columns


def test_load_parquet(jsonl_dataset: str, mod_tmp_path: Path):
    pq_path = mod_tmp_path / "dset.parquet"

    dset = load_parquet(
        pattern=jsonl_dataset,
        extract=extract_jsonl,
        where=pq_path,
    )
    assert len(dset.files) == 1

    df = dset.read().to_pandas()
    assert df.shape == (NUM_BATCHES * BATCH_SIZE, 7)

    with pytest.raises(FileExistsError):
        load_parquet(
            pattern=jsonl_dataset,
            extract=extract_jsonl,
            where=pq_path,
        )

    # Re-write batch 0
    _random_jsonl_batch(0, mod_tmp_path, BATCH_SIZE)
    # New batch
    _random_jsonl_batch(NUM_BATCHES, mod_tmp_path, BATCH_SIZE)

    dset2 = load_parquet(
        pattern=jsonl_dataset,
        extract=extract_jsonl,
        where=pq_path,
        incremental=True,
    )
    assert len(dset2.files) == 2

    df2 = dset2.read().to_pandas()
    assert df2.shape == ((NUM_BATCHES + 2) * BATCH_SIZE, 7)


def test_load_parquet_parallel(jsonl_dataset: str, mod_tmp_path: Path):
    pq_path = mod_tmp_path / "dset_parallel.parquet"

    dset = load_parquet(
        pattern=jsonl_dataset,
        extract=extract_jsonl,
        where=pq_path,
        workers=2,
    )
    assert len(dset.files) == 2

    df = dset.read().to_pandas()
    # NOTE: only + 1 bc the new batch is no longer new
    assert df.shape == ((NUM_BATCHES + 1) * BATCH_SIZE, 7)


if __name__ == "__main__":
    pytest.main([__file__])
