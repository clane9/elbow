import re
from typing import Any, Dict, Optional, Union

import numpy as np
import pyarrow as pa
from typing_extensions import get_args, get_origin

from . import PaJSONType, PaNDArrayType, PaPickleType

__all__ = ["DataType", "Fields", "get_dtype", "infer_dtype"]

DataType = Union[type, str, pa.DataType, np.dtype]
Fields = Dict[str, DataType]


def get_dtype(alias: DataType) -> pa.DataType:
    """
    Attempt to infer the PyArrow dtype from string type alias or numpy dtype.

    The list of available pyarrow type aliases is available `here`_.

    The following nested type aliases are also supported:

    - ``"array<TYPE>"`` -> ``pa.list_(TYPE)``
    - ``"list<(item:)? TYPE>"`` -> ``pa.list_(TYPE)``
    - ``"struct<NAME: TYPE, ...>"`` -> ``pa.struct({NAME: TYPE, ...})``

    The following extension types are also supported:

    - ``"json"`` -> ``PaJSONType()``
    - ``"pickle"`` -> ``PaPickleType()``
    - ``"ndarray<(item:)? TYPE>"`` -> ``PaNDArrayType(TYPE)``

    .. _here: https://github.com/apache/arrow/blob/go/v10.0.0/python/pyarrow/types.pxi#L3159

    NOTE: nested types containing extension types are not supported
    """
    if isinstance(alias, dict):
        return _struct_from_fields(alias)

    elif not isinstance(alias, str):
        return _get_primitive_dtype(alias)

    alias = alias.strip()

    if alias == "json":
        return PaJSONType()

    if alias == "pickle":
        return PaPickleType()

    dtype = _struct_from_string(alias)
    if dtype is not None:
        return dtype

    dtype = _list_from_string(alias)
    if dtype is not None:
        return dtype

    dtype = _ndarray_from_string(alias)
    if dtype is not None:
        return dtype

    return _get_primitive_dtype(alias)


def _get_primitive_dtype(dtype: DataType) -> pa.DataType:
    # Handle aliases of the form Optional[str]
    dtype = _unbox_primitive_optional(dtype)

    try:
        return pa.lib.ensure_type(dtype)
    except Exception:
        pass

    try:
        return pa.from_numpy_dtype(dtype)
    except Exception:
        pass

    raise ValueError(f"Unsupported dtype '{dtype}'")


def _unbox_primitive_optional(dtype: DataType) -> DataType:
    """
    Unbox type aliases of the form `Optional[str]`.
    """
    if _is_primitive_optional(dtype):
        dtype = get_args(dtype)[0]
    return dtype


def _is_primitive_optional(dtype: DataType) -> bool:
    """
    Check if dtype is an optional of a primitive type like `Optional[str]`.
    """
    return (
        get_origin(dtype) is Union
        and len(get_args(dtype)) == 2
        and isinstance(None, get_args(dtype)[1])
    )


def _struct_from_string(alias: str) -> Optional[pa.DataType]:
    match = re.match(r"^struct\s*<(.+)>$", alias)
    if match is None:
        return None

    fields = []
    items = match.group(1)
    try:
        while items:
            end = _find_split(items)
            if end == -1:
                item, items = items, ""
            else:
                item, items = items[:end], items[(end + 1) :]

            # Split just on the first ":" in case you have a field like
            # "data: list<item: double>"
            split = item.find(":")
            if split < 0:
                raise ValueError
            name, item_alias = item[:split], item[(split + 1) :]
            fields.append((name.strip(), get_dtype(item_alias)))
    except ValueError as exc:
        raise ValueError(f"Invalid struct alias {alias}") from exc
    return pa.struct(fields)


def _struct_from_fields(fields: Fields) -> pa.DataType:
    """
    Construct struct type from a list or dict of fields.
    """
    return pa.struct({k: get_dtype(v) for k, v in fields.items()})


def _find_split(items: str):
    """
    Find next comma split, ignoring regions nested in ``"< >"`` (which can contain
    commas that well mess up parsing in the case of nested structs).
    """
    nest_count = 0
    for ii, c in enumerate(items):
        if c == "<":
            nest_count += 1
        elif c == ">":
            nest_count -= 1
        elif c == "," and nest_count == 0:
            return ii
    return -1


def _list_from_string(alias: str) -> Optional[pa.DataType]:
    match = re.match(r"^(?:list|array)\s*<(?:\s*item\s*:)?(.+)>$", alias)
    if match is None:
        return None
    alias = match.group(1)
    dtype = get_dtype(alias)
    return pa.list_(dtype)


def _ndarray_from_string(alias: str) -> Optional[pa.DataType]:
    match = re.match(r"^ndarray\s*<(?:\s*item\s*:)?(.+)>$", alias)
    if match is None:
        return None
    alias = match.group(1)
    dtype = get_dtype(alias)
    return PaNDArrayType(dtype)


def infer_dtype(scalar: Any) -> pa.DataType:
    """
    Attempt to infer the data type of an arbitrary scalar value.
    """
    if isinstance(scalar, np.ndarray) and scalar.ndim > 1:
        return PaNDArrayType(get_dtype(scalar.dtype))

    return pa.scalar(scalar).type
