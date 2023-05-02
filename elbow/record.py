from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Union

import numpy as np
import pandas as pd
import pyarrow as pa

from elbow.dtypes import DataType, PaExtensionType, get_dtype, infer_dtype
from elbow.typing import Dataclass

__all__ = [
    "RecordLike",
    "Record",
    "RecordBatch",
    "as_record",
    "is_recordlike",
    "concat",
    "arrow_record",
    "arrow_table",
    "arrow_array",
]

RecordLike = Union[Dict[str, Any], Dataclass, "Record"]


class Record(dict):
    """
    A thin wrapper around a plain dict with typed fields and supporting conversion to
    PyArrow.

    Example::

        record = Record({"name": "abc", "age": 12}, types={"age": "int32"})
        # pyarrow schema
        schema = record.arrow_schema()
        # to pyarrow RecordBatch
        batch = record.to_arrow(schema=schema)

        # concatenate records
        other = Record({"height": 74.0})
        record = record + other

    TODO: Support metadata? This might be nice to propagate into arrow.
    """

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        types: Optional[Dict[str, DataType]] = None,
    ):
        """
        Construct a Record.

        Args:
            data: mapping of names to values
            types: optional mapping of names to data types. Data types can be any type
                or alias accepted by `get_dtype()`.
        """
        if data is None:
            data = {}
        if types is None:
            types = {}
        elif not set(types).issubset(data):
            raise ValueError("types do not match data")

        super().__init__(data)
        self._types = dict(types)

    def type(self, key: str) -> Optional[DataType]:
        """
        Get the annotated type for field `key`.
        """
        return self._types.get(key)

    def arrow_type(self, key: str) -> pa.DataType:
        """
        Get the arrow data type for field `key`.
        """
        if key in self._types:
            typ = get_dtype(self._types[key])
        else:
            typ = infer_dtype(self[key])
        return typ

    def arrow_schema(self) -> pa.Schema:
        """
        Return a PyArrow schema for the record.
        """
        return pa.schema({k: self.arrow_type(k) for k in self})

    def to_arrow(self, schema: Optional[pa.Schema] = None) -> pa.RecordBatch:
        """
        Convert the record to a PyArrow RecordBatch.
        """
        if schema is None:
            schema = self.arrow_schema()
        return arrow_record(self, schema)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the record to a plain dict.
        """
        return {**self}

    def merge(self, other: Any) -> "Record":
        """
        Merge with another record.
        """
        return _merge(self, other)

    @classmethod
    def from_dataclass(cls, obj: Any):
        """
        Create a Record from a dataclass instance.
        """
        obj_fields = fields(obj)
        data = {f.name: getattr(obj, f.name) for f in obj_fields}
        types: Dict[str, DataType] = {
            f.name: f.type for f in obj_fields if f.type not in {None, Any}
        }
        return cls(data, types=types)

    __add__ = merge


class RecordBatch:
    """
    An incremental batch of records.

    Example::

        batch = RecordBatch()
        for record in stream():
            batch.append(record)

        table = batch.to_arrow()

    Args:
        batch: batch of initial records.
        schema: PyArrow Schema or mapping of column names to types. If absent, the
            schema is initialized from the first record.
        strict: by default, the schema is updated incrementally to contain the union of
            columns present in all records. Setting `strict` to `True` disables this.
            All records must then share the same columns.
    """

    def __init__(
        self,
        batch: Optional[Iterable[RecordLike]] = None,
        schema: Optional[Union[Dict[str, DataType], pa.Schema]] = None,
        strict: bool = False,
    ):
        self.schema = schema
        self.strict = strict

        self.reset()
        if batch is not None:
            self.extend(batch)

    def reset(self) -> None:
        """
        Reset the batch.
        """
        self._batch: List[Record] = []
        self._columns: List[str] = []
        self._fields: Dict[str, pa.DataType] = {}
        self._null_fields: Set[str] = set()

        if self.schema is not None:
            schema = self.schema
            if not isinstance(schema, pa.Schema):
                schema = pa.schema(
                    {name: get_dtype(typ) for name, typ in schema.items()}
                )
            self._init_schema(schema)

    def append(self, record: RecordLike):
        """
        Append a record to the batch.
        """
        record = as_record(record)

        if not self._fields:
            self._init_schema(record.arrow_schema())

        new_columns = self._new_columns(record)
        if new_columns:
            if self.strict:
                raise ValueError(
                    f"Record contains new columns {new_columns} not present in batch"
                )
            self._add_fields_from_record(record, new_columns)

        if self._contains_null():
            self._update_null_from_record(record)

        self._batch.append(record)

    def extend(self, records: Iterable[RecordLike]):
        """
        Extend the batch with records.
        """
        for record in records:
            self.append(record)

    def _init_schema(self, schema: pa.Schema):
        """
        Initialize the internal batch schema.
        """
        self._columns = list(schema.names)
        self._fields = {field.name: field.type for field in schema}
        self._null_fields = {
            name for name, typ in self._fields.items() if pa.types.is_null(typ)
        }

    def _new_columns(self, record: Record) -> List[str]:
        return [k for k in record if k not in self._fields]

    def _contains_null(self) -> bool:
        return len(self._null_fields) > 0

    def _add_fields_from_record(self, record: Record, new_columns: List[str]):
        # TODO: Might want to try preserving the relative ordering at some point.
        # but pandas doesn't even do this so it can wait.
        for name in new_columns:
            self._columns.append(name)
            self._fields[name] = typ = record.arrow_type(name)
            if pa.types.is_null(typ):
                self._null_fields.add(name)

    def _update_null_from_record(self, record: Record):
        null_fields = self._null_fields.copy()
        for name in null_fields:
            if name in record:
                typ = record.arrow_type(name)
                if not pa.types.is_null(typ):
                    self._fields[name] = typ
                    self._null_fields.remove(name)

    def arrow_schema(self) -> pa.Schema:
        """
        Return a PyArrow schema for the batch.
        """
        schema = pa.schema({name: self._fields[name] for name in self._columns})
        return schema

    def to_arrow(self) -> pa.Table:
        """
        Convert the batch to a PyArrow Table.
        """
        schema = self.arrow_schema()
        table = arrow_table(self._batch, schema)
        return table

    def to_df(self) -> pd.DataFrame:
        """
        Convert the batch to a pandas DataFrame.
        """
        return self.to_arrow().to_pandas()

    def clear(self):
        """
        Empty the batch.
        """
        self._batch.clear()

    def __len__(self) -> int:
        return len(self._batch)


def concat(objs: Iterable[RecordLike]) -> Record:
    """
    Concatenate multiple RecordLikes into a single record.
    """
    rec = Record()
    for other in objs:
        rec = _merge(rec, other)
    return rec


def _merge(rec: RecordLike, other: RecordLike) -> Record:
    """
    Merge one RecordLike with another.
    """
    rec = as_record(rec)
    other = as_record(other)

    if not set(rec).isdisjoint(other):
        raise ValueError("Can't merge records with overlapping fields")

    data = {**rec, **other}
    types = {**rec._types, **other._types}
    return Record(data, types)


def as_record(obj: RecordLike) -> Record:
    """
    Construct a Record from a Record-like object (e.g. a dict or dataclass instance).
    """
    if isinstance(obj, Record):
        rec = obj
    elif is_dataclass(obj):
        rec = Record.from_dataclass(obj)
    elif isinstance(obj, Mapping):
        rec = Record(data=obj)
    else:
        raise TypeError("Object cannot be cast to a record")
    return rec


def is_recordlike(obj: Any) -> bool:
    """
    Check if an object can be cast to a Record.
    """
    return isinstance(obj, (Mapping, Record)) or is_dataclass(obj)


def arrow_record(data: RecordLike, schema: pa.Schema) -> pa.RecordBatch:
    """
    Construct a PyArrow RecordBatch for `data`.
    """
    data = as_record(data)

    row = {}
    for field in schema:
        name, typ = field.name, field.type
        value = data.get(name)
        if isinstance(typ, PaExtensionType):
            value = typ.pack(value)
        row[name] = value
    batch = pa.RecordBatch.from_pylist([row], schema=schema)
    return batch


def arrow_table(data: Iterable[RecordLike], schema: pa.Schema):
    """
    Simplified wrapper around `pa.table()` for converting a list of records to a PyArrow
    Table, with support for extension types.
    """
    recs = [as_record(row) for row in data]

    table = {}
    for field in schema:
        name, typ = field.name, field.type
        arr = [rec.get(name) for rec in recs]
        arr = arrow_array(arr, type=typ)
        table[name] = arr
    table = pa.table(table, schema=schema)
    return table


def arrow_array(
    data: Union[Iterable, np.ndarray, pd.Series],
    type: pa.DataType,
) -> pa.Array:
    """
    Wrapper around `pa.array()` with support for extension types.
    """
    if isinstance(type, PaExtensionType):
        data = [type.pack(v) for v in data]
    return pa.array(data, type=type)
