import string
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from pyarrow import parquet as pq

from elbow.record import RecordLike
from elbow.sinks import BufferedParquetWriter


def _random_record(rng: np.random.Generator) -> RecordLike:
    rec = {
        "a": rng.integers(0, 10),
        "b": rng.random(),
        "c": _random_string(rng, 5),
        "d": rng.normal(size=rng.integers(0, 100)),
    }
    return rec


def _random_string(rng: np.random.Generator, length: int):
    return "".join(rng.choice(list(string.ascii_letters), length))


def test_buffered_parquet_writer(tmp_path: Path):
    rng = np.random.default_rng(2022)
    table_path = str(tmp_path / "table.parquet")
    num_records = 10000

    with BufferedParquetWriter(table_path, buffer_size="1 MB") as writer:
        for _ in range(num_records):
            rec = _random_record(rng)
            writer.write(rec)

    table = pq.read_table(table_path)
    assert table.shape == (num_records, 4)

    expected_schema = pa.schema(
        {
            "a": pa.int64(),
            "b": pa.float64(),
            "c": pa.string(),
            "d": pa.list_(pa.float64()),
        },
    )
    assert table.schema.equals(expected_schema)

    # TODO: is this reproducible?
    assert table.get_total_buffer_size() == 4304773
    # TODO: why are these two totals different?
    assert writer.total_bytes() == 4240832


if __name__ == "__main__":
    pytest.main([__file__])
