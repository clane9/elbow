from typing import BinaryIO, Optional, Union

import pyarrow as pa
from pyarrow import parquet as pq

from elbow.record import RecordBatch, RecordLike
from elbow.typing import StrOrPath
from elbow.utils import parse_size

__all__ = ["BufferedParquetWriter"]


class BufferedParquetWriter:
    """
    Write a stream of records to a parquet file with a buffer.

    Example::

        with BufferedParquetWriter("table.parquet") as writer:
            for record in stream:
                writer.write(record)

    Args:
        where: path to parquet output file or file-like object.
        schema: optional pyarrow schema. If absent, the schema will be inferred from the
            first batch of records.
        batch_size: internal record batch size. Setting a larger value increases the
            window to infer the schema.
        buffer_size: size of the internal table buffer, consisting of one or more
            batches. Either an int number of bytes, or a string representing a buffer
            size, e.g. "64 MiB".
        **kwargs: pass-through kwargs to `pyarrow.parquet.ParquetWriter()`.
    """

    def __init__(
        self,
        where: Union[StrOrPath, BinaryIO],
        schema: Optional[pa.Schema] = None,
        buffer_size: Union[str, int] = "64 MiB",
        batch_size: int = 256,
        **kwargs,
    ):
        self.where = where
        self.schema = schema
        self.buffer_size = buffer_size
        self.batch_size = batch_size

        if isinstance(buffer_size, str):
            self._buffer_size_bytes = parse_size(buffer_size)
        else:
            self._buffer_size_bytes = buffer_size

        self._writer: Optional[pq.ParquetWriter] = None
        self._writer_kwargs = kwargs
        self._batch = RecordBatch(schema=schema, strict=(schema is not None))
        self._table: Optional[pa.Table] = None
        self._schema: Optional[pa.Schema] = schema
        self._total_bytes = 0
        self._buffer_bytes = 0

    def write(self, record: RecordLike):
        """
        Write a record.
        """
        self._batch.append(record)

        # Extend buffer table with current batch
        if len(self._batch) >= self.batch_size:
            self._push_batch()

        if self._buffer_bytes > self._buffer_size_bytes:
            self.flush()

    def _push_batch(self):
        """
        Push a batch onto the buffer table.
        """
        if len(self._batch) > 0:
            batch_table = self._batch.to_arrow()

            # Fix schema from initial batch.
            if self._schema is None:
                self._schema = batch_table.schema

            if self._table is None:
                self._table = batch_table
            else:
                self._table = pa.concat_tables([self._table, batch_table])

            self._buffer_bytes += batch_table.get_total_buffer_size()

            # For all subsequent batches, use a strict schema
            self._batch = RecordBatch(schema=self._schema, strict=True)

    def flush(self):
        """
        Flush the table buffer.
        """
        self._push_batch()

        if self._table is not None:
            if self._writer is None:
                # TODO: Might consider writing to a temp file initially, in particular
                # to avoid race conditions when generating parquets incrementally with
                # multiple workers.
                self._writer = pq.ParquetWriter(
                    where=self.where,
                    schema=self._schema,
                    **self._writer_kwargs,
                )

            self._writer.write(
                self._table, row_group_size=(2 * self._buffer_size_bytes)
            )
            self._total_bytes += self._table.get_total_buffer_size()
            self._table = None
            self._buffer_bytes = 0

    def close(self):
        """
        Flush the buffer and close the writer.
        """
        self.flush()
        if self._writer is not None:
            self._writer.close()

    def total_bytes(self) -> int:
        """
        Total bytes written plus current buffer size.
        """
        return self._total_bytes + self._buffer_bytes

    def __enter__(self) -> "BufferedParquetWriter":
        return self

    def __exit__(self, *args):
        self.close()

    __call__ = write
