from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from pyarrow import parquet as pq

from elbow.sinks import BufferedParquetWriter
from tests.utils_for_tests import random_record


def test_buffered_parquet_writer(tmp_path: Path):
    rng = np.random.default_rng(2022)
    table_path = str(tmp_path / "table.parquet")
    num_records = 10000

    with BufferedParquetWriter(table_path, buffer_size="1 MB") as writer:
        for _ in range(num_records):
            rec = random_record(rng)
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
    assert table.get_total_buffer_size() == 4582313
    # TODO: why are these two totals different?
    assert writer.total_bytes() == 4518248


if __name__ == "__main__":
    pytest.main([__file__])
