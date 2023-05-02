import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from functools import partial
from glob import iglob
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

from elbow.extractors import Extractor
from elbow.filters import FileModifiedIndex, hash_partitioner
from elbow.pipeline import Pipeline
from elbow.record import RecordBatch
from elbow.sinks import BufferedParquetWriter
from elbow.typing import StrOrPath
from elbow.utils import atomicopen

__all__ = ["load_table", "load_parquet"]


def load_table(
    pattern: str,
    extract: Extractor,
    recursive: bool = True,
    max_failures: Optional[int] = 0,
) -> pd.DataFrame:
    """
    Extract records from a stream of files and load into a pandas DataFrame

    Args:
        pattern: shell-style file pattern as in `glob.glob()`
        extract: extract function mapping file paths to records
        recursive: if True, patterns containing '**' will match any files and zero or
            more directories
        max_failures: number of failures to tolerate

    Returns:
        A DataFrame containing the concatenated records (in arbitrary order)
    """
    source = iglob(pattern, recursive=recursive)
    batch = RecordBatch()
    pipe = Pipeline(
        source=source, extract=extract, sink=batch.append, max_failures=max_failures
    )
    pipe.run()

    df = batch.to_df()
    return df


def load_parquet(
    pattern: str,
    extract: Extractor,
    where: StrOrPath,
    recursive: bool = True,
    incremental: bool = False,
    workers: Optional[int] = None,
    max_failures: Optional[int] = 0,
) -> pq.ParquetDataset:
    """
    Extract records from a stream of files and load as a Parquet dataset

    Args:
        pattern: shell-style file pattern as in `glob.glob()`
        extract: extract function mapping file paths to records
        where: path to output parquet dataset directory
        recursive: if True, patterns containing '**' will match any files and zero or
            more directories
        incremental: update dataset incrementally with only new or changed files.
        workers: number of parallel processes. If `None` or 1, run in the main
            process. Setting to -1 runs in `os.cpu_count()` processes.
        max_failures: number of failures to tolerate

    Returns:
        A PyArrow ParquetDataset handle to the loaded dataset
    """
    if not incremental and Path(where).exists():
        raise FileExistsError(f"Parquet output directory {where} already exists")

    if workers is None or workers == 0:
        workers = 1
    elif workers < 0:
        workers = os.cpu_count()

    _worker = partial(
        _load_parquet_worker,
        pattern=pattern,
        extract=extract,
        where=where,
        incremental=incremental,
        workers=workers,
        recursive=recursive,
        max_failures=max_failures,
    )

    if workers > 1:
        with ProcessPoolExecutor(workers) as pool:
            pool.map(_worker, range(workers))
            pool.shutdown()
    else:
        _worker(0)

    dset = pq.ParquetDataset(where)
    return dset


def _load_parquet_worker(
    worker_id: int,
    *,
    pattern: str,
    extract: Extractor,
    where: StrOrPath,
    incremental: bool,
    workers: int,
    recursive: bool,
    max_failures: Optional[int],
):
    start = datetime.now()
    where = Path(where)
    source = iglob(pattern, recursive=recursive)

    if incremental and where.exists():
        # NOTE: Race to read index while other workers try to write.
        # But it shouldn't matter since each worker gets a unique partition (?).
        file_mod_index = FileModifiedIndex.from_parquet(where)
        source = filter(file_mod_index, source)

    # TODO: maybe let user specify partition key function? By default we will get
    # random assignment of paths to workers.
    if workers > 1:
        partitioner = hash_partitioner(worker_id, workers)
        source = filter(partitioner, source)

    # Include start time in file name in case of multiple incremental loads.
    where = where / f"part-{start.strftime('%Y%m%d%H%M%S')}-{worker_id:04d}"
    where.parent.mkdir(parents=True, exist_ok=True)

    # Using atomicopen to avoid partial output files
    with atomicopen(where, "wb") as f:
        with BufferedParquetWriter(where=f) as writer:
            # TODO: should this just be a function?
            pipe = Pipeline(
                source=source, extract=extract, sink=writer, max_failures=max_failures
            )
            pipe.run()
