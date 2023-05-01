from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pytest

from elbow import record


@dataclass
class Record:
    a: int
    b: float
    c: Any


def test_record_types():
    rec = record.Record({"a": 1, "b": 2.3}, types={"a": "int32"})

    assert rec.type("a") == "int32"
    assert rec.type("b") is None

    expected_schema = pa.schema({"a": pa.int32(), "b": pa.float64()})
    assert rec.arrow_schema().equals(expected_schema)

    # type keys don't match data
    with pytest.raises(ValueError):
        record.Record({"a": 1}, types={"b": int})


def test_record_to_arrow():
    rec = record.Record({"a": 1, "b": 2.3})
    arrow_rec = rec.to_arrow()
    assert arrow_rec.to_pylist()[0] == rec


def test_record_to_dict():
    rec = record.Record({"a": 1, "b": 2.3})
    dict_rec = rec.to_dict()
    assert rec == dict_rec
    assert type(dict_rec) == dict


def test_record_merge():
    rec_a = record.Record({"a": 1, "b": 2.3}, types={"a": "int32"})
    rec_b = record.Record({"c": "abc", "d": None}, types={"d": str})
    rec = rec_a + rec_b

    assert list(rec.keys()) == ["a", "b", "c", "d"]
    assert list(rec.values()) == [1, 2.3, "abc", None]
    assert rec.type("d") == str

    rec_c = {"a": None}
    with pytest.raises(ValueError):
        rec_a + rec_c


def test_record_from_dataclass():
    rec = record.Record.from_dataclass(Record(1, 2.3, "abc"))

    assert rec.type("a") == int
    assert rec.type("b") == float
    assert rec.type("c") is None


def test_as_record():
    data = {"a": 1, "b": 2.3, "c": "abc"}
    rec1 = record.as_record(data)  # from dict
    rec2 = record.as_record(Record(**data))  # from dataclass
    rec3 = record.as_record(rec1)  # from other record (no copy)

    assert rec1 == data
    assert rec2 == data
    assert rec3 == data

    # Not a RecordLike
    non_rec = [1, 2, 3]
    assert not record.is_recordlike(non_rec)
    with pytest.raises(TypeError):
        record.as_record(non_rec)


def test_concat():
    rec = record.concat([{"a": 1, "b": 2.3}, {"c": "abc"}, {"d": "def"}])
    assert list(rec.keys()) == ["a", "b", "c", "d"]


def test_record_batch():
    recs = [
        {"a": 1, "b": 2.3},
        {"a": 2, "c": "abc", "d": None},  # new column, with null
        {"b": 3.4, "d": "def"},  # update null type
    ]

    batch = record.RecordBatch(schema={"a": "int32"}, strict=False)
    batch.extend(recs)

    assert len(batch) == 3

    expected_schema = pa.schema(
        {"a": pa.int32(), "b": pa.float64(), "c": pa.string(), "d": pa.string()}
    )
    assert batch.arrow_schema().equals(expected_schema)

    df = batch.to_df()
    assert df.shape == (3, 4)
    assert df.columns.tolist() == ["a", "b", "c", "d"]
    assert df["d"].isna().sum() == 2


if __name__ == "__main__":
    pytest.main(["-x", __file__])
