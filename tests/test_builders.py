import time
from pathlib import Path

import pandas as pd
import pytest
from pyarrow import parquet as pq
from pytest_benchmark.fixture import BenchmarkFixture

from elbow.builders import build_parquet, build_table
from elbow.sources.filesystem import Crawler
from tests.utils_for_tests import extract_jsonl, random_jsonl_batch

NUM_BATCHES = 64
BATCH_SIZE = 256
SEED = 2022


@pytest.fixture(scope="module")
def mod_tmp_path(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("tmp")


@pytest.fixture(scope="module")
def jsonl_dataset(mod_tmp_path: Path) -> str:
    for ii in range(NUM_BATCHES):
        random_jsonl_batch(mod_tmp_path, BATCH_SIZE, seed=(SEED + ii))

    pattern = str(mod_tmp_path / "*.json")
    return pattern


def test_build_table(jsonl_dataset: str, benchmark: BenchmarkFixture):
    df = benchmark(build_table, source=jsonl_dataset, extract=extract_jsonl)
    assert df.shape == (NUM_BATCHES * BATCH_SIZE, 7)

    expected_columns = ["file_path", "link_target", "mod_time", "a", "b", "c", "d"]
    assert df.columns.tolist() == expected_columns


def test_build_table_parallel(jsonl_dataset: str):
    df = build_table(source=jsonl_dataset, extract=extract_jsonl, workers=2)
    assert df.shape == (NUM_BATCHES * BATCH_SIZE, 7)

    expected_columns = ["file_path", "link_target", "mod_time", "a", "b", "c", "d"]
    assert df.columns.tolist() == expected_columns


def test_build_parquet(jsonl_dataset: str, mod_tmp_path: Path):
    pq_path = mod_tmp_path / "dset.pqds"

    build_parquet(source=jsonl_dataset, extract=extract_jsonl, output=pq_path)
    dset = pq.ParquetDataset(pq_path)
    assert len(dset.files) == 1

    df = dset.read().to_pandas()
    assert df.shape == (NUM_BATCHES * BATCH_SIZE, 7)

    with pytest.raises(FileExistsError):
        build_parquet(source=jsonl_dataset, extract=extract_jsonl, output=pq_path)

    # Re-write batch 0
    random_jsonl_batch(mod_tmp_path, BATCH_SIZE, seed=SEED)
    # New batch
    random_jsonl_batch(mod_tmp_path, BATCH_SIZE, seed=(SEED + NUM_BATCHES))

    # NOTE: have to wait at least a second to avoid clobbering the previous partition.
    time.sleep(1.0)

    build_parquet(
        source=jsonl_dataset, extract=extract_jsonl, output=pq_path, incremental=True
    )
    dset2 = pq.ParquetDataset(pq_path)
    assert len(dset2.files) == 2

    df2 = dset2.read().to_pandas()
    assert df2.shape == ((NUM_BATCHES + 2) * BATCH_SIZE, 7)


def test_build_parquet_parallel(jsonl_dataset: str, mod_tmp_path: Path):
    pq_path = mod_tmp_path / "dset_parallel.pqds"

    build_parquet(
        source=jsonl_dataset, extract=extract_jsonl, output=pq_path, workers=2
    )
    dset = pq.ParquetDataset(pq_path)
    assert len(dset.files) == 2

    df = dset.read().to_pandas()
    # NOTE: only + 1 rather than + 2 bc the new batch is no longer new
    assert df.shape == ((NUM_BATCHES + 1) * BATCH_SIZE, 7)


def test_build_parquet_partial(jsonl_dataset: str, mod_tmp_path: Path):
    pq_path = mod_tmp_path / "dset_partial.pqds"

    build_parquet(
        source=jsonl_dataset,
        extract=extract_jsonl,
        output=pq_path,
        workers=2,
        worker_id=0,
    )
    build_parquet(
        source=jsonl_dataset,
        extract=extract_jsonl,
        output=pq_path,
        workers=2,
        worker_id=1,
    )
    dset = pq.ParquetDataset(pq_path)
    assert len(dset.files) == 2

    df = dset.read().to_pandas()
    # NOTE: only + 1 rather than + 2 bc the new batch is no longer new
    assert df.shape == ((NUM_BATCHES + 1) * BATCH_SIZE, 7)


def test_build_parquet_with_crawl(mod_tmp_path: Path):
    pq_path = mod_tmp_path / "dset_crawl.pqds"

    source = Crawler(
        root=mod_tmp_path,
        include=["*.json"],
        files_only=True,
    )
    build_parquet(
        source=source,
        extract=extract_jsonl,
        output=pq_path,
        workers=2,
    )
    df = pd.read_parquet(pq_path)
    # NOTE: only + 1 rather than + 2 bc the new batch is no longer new
    assert df.shape == ((NUM_BATCHES + 1) * BATCH_SIZE, 7)


if __name__ == "__main__":
    pytest.main([__file__])
