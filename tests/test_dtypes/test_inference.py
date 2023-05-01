from typing import Optional

import numpy as np
import pyarrow as pa
import pytest

from elbow.dtypes import DataType, PaJSONType, PaNDArrayType, PaPickleType, get_dtype


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("int", pa.int64()),
        ("int16", pa.int16()),
        ("str", pa.string()),
        (np.float32, pa.float32()),
        ("datetime64[ns]", pa.timestamp("ns")),
        ("list<item: float32>", pa.list_(pa.float32())),
        (
            "struct < A: int, B: str >",
            pa.struct({"A": pa.int64(), "B": pa.string()}),
        ),
        (
            "struct< data: list<double>, shape: list<int64> >",
            pa.struct({"data": pa.list_(pa.float64()), "shape": pa.list_(pa.int64())}),
        ),
        # nested struct field
        (
            (
                "struct< A: struct< a: int32, b: float32 >, "
                "B: list<float32> , C: struct<c: list<int32>> >"
            ),
            pa.struct(
                {
                    "A": pa.struct({"a": pa.int32(), "b": pa.float32()}),
                    "B": pa.list_(pa.float32()),
                    "C": pa.struct({"c": pa.list_(pa.int32())}),
                }
            ),
        ),
        ("json", PaJSONType()),
        ("pickle", PaPickleType()),
        ("ndarray<float32>", PaNDArrayType(pa.float32())),
        (Optional[str], pa.string()),
    ],
)
def test_get_dtype(test_input: DataType, expected: pa.DataType):
    assert get_dtype(test_input) == expected


@pytest.mark.parametrize(
    "unsupported_dtype", [object, "object", "map<int32, str>", "list", "struct"]
)
def test_unsupported_get_dtype(unsupported_dtype: DataType):
    with pytest.raises(ValueError):
        get_dtype(unsupported_dtype)


if __name__ == "__main__":
    pytest.main(["-x", __file__])
