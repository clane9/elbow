import logging
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from glob import iglob
from pathlib import Path
from typing import Iterable, Optional, Union

import pandas as pd

from elbow.extractors import Extractor
from elbow.filters import FileModifiedIndex, hash_partitioner
from elbow.pipeline import Pipeline
from elbow.record import RecordBatch
from elbow.sinks import BufferedParquetWriter
from elbow.typing import StrOrPath
from elbow.utils import atomicopen, cpu_count, setup_logging

__all__ = ["build_table", "build_parquet"]


def build_table(
    source: Union[str, Iterable[StrOrPath]],
    extract: Extractor,
    *,
    max_failures: Optional[int] = 0,
) -> pd.DataFrame:
    """
    Extract records from a stream of files and load into a pandas DataFrame

    Args:
        source: shell-style file pattern as in `glob.glob()` or iterable of paths.
            Patterns containing '**' will match any files and zero or more directories
        extract: extract function mapping file paths to records
        max_failures: number of failures to tolerate

    Returns:
        A DataFrame containing the concatenated records (in arbitrary order)
    """
    if isinstance(source, str):
        source = iglob(source, recursive=True)

    batch = RecordBatch()
    pipe = Pipeline(
        source=source, extract=extract, sink=batch.append, max_failures=max_failures
    )
    pipe.run()

    df = batch.to_df()
    return df


def build_parquet(
    source: Union[str, Iterable[StrOrPath]],
    extract: Extractor,
    where: StrOrPath,
    *,
    incremental: bool = False,
    overwrite: bool = False,
    workers: Optional[int] = None,
    worker_id: Optional[int] = None,
    max_failures: Optional[int] = 0,
) -> None:
    """
    Extract records from a stream of files and load as a Parquet dataset

    Args:
        source: shell-style file pattern as in `glob.glob()` or iterable of paths.
            Patterns containing '**' will match any files and zero or more directories
        extract: extract function mapping file paths to records
        where: path to output parquet dataset directory
        incremental: update dataset incrementally with only new or changed files.
        overwrite: overwrite previous results.
        workers: number of parallel processes. If `None` or 1, run in the main
            process. Setting to <= 0 runs as many processes as there are cores
            available.
        worker_id: optional worker ID to use when scheduling parallel tasks externally.
            Specifying the number of workers is required in this case. Incompatible with
            overwrite.
        max_failures: number of extract failures to tolerate
    """
    # TODO:
    #     - generalize sources
    #     - parallel extraction is a bit awkward due to hashing assignment might consider
    #       pre-expanding the sources and partitioning. But this is susceptible to racing.

    if worker_id is not None:
        if overwrite:
            raise ValueError("Can't overwrite when using worker_id")
        if workers is None or workers <= 0:
            raise ValueError("workers > 0 is required when using worker_id")

    inplace = incremental or worker_id is not None
    if Path(where).exists() and not inplace:
        if overwrite:
            shutil.rmtree(where)
        else:
            raise FileExistsError(f"Parquet output directory {where} already exists")

    if workers is None:
        workers = 1
    elif workers <= 0:
        workers = cpu_count()

    _worker = partial(
        _build_parquet_worker,
        source=source,
        extract=extract,
        where=where,
        incremental=incremental,
        workers=workers,
        max_failures=max_failures,
        log_level=logging.getLogger().level,
    )

    if workers == 1:
        _worker(0)
    elif worker_id is not None:
        _worker(worker_id)
    else:
        with ProcessPoolExecutor(workers) as pool:
            futures_to_id = {pool.submit(_worker, ii): ii for ii in range(workers)}

            for future in as_completed(futures_to_id):
                try:
                    future.result()
                except Exception as exc:
                    worker_id = futures_to_id[future]
                    logging.warning(
                        "Generated exception in worker %d", worker_id, exc_info=exc
                    )


def _build_parquet_worker(
    worker_id: int,
    *,
    source: Union[str, Iterable[StrOrPath]],
    extract: Extractor,
    where: StrOrPath,
    incremental: bool,
    workers: int,
    max_failures: Optional[int],
    log_level: int,
):
    setup_logging(log_level)

    start = datetime.now()
    where = Path(where)
    if isinstance(source, str):
        source = iglob(source, recursive=True)

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
    where = where / f"part-{start.strftime('%Y%m%d%H%M%S')}-{worker_id:04d}.parquet"
    if where.exists():
        raise FileExistsError(f"Partition {where} already exists")
    where.parent.mkdir(parents=True, exist_ok=True)

    # Using atomicopen to avoid partial output files and empty file errors.
    with atomicopen(where, "wb") as f:
        with BufferedParquetWriter(where=f) as writer:
            # TODO: should this just be a function?
            pipe = Pipeline(
                source=source, extract=extract, sink=writer, max_failures=max_failures
            )
            counts = pipe.run()

    return counts
