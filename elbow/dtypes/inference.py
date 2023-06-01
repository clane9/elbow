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
    Attempt to infer the PyArrow dtype from string type alias or numpy dtype or python
    type hint.

    The list of available pyarrow type aliases is available `here`_.

    The following nested type aliases are also supported:

    - ``"array<TYPE>"`` -> ``pa.list_(TYPE)``
    - ``"list<(item:)? TYPE>"`` -> ``pa.list_(TYPE)``
    - ``"struct<NAME: TYPE, ...>"`` -> ``pa.struct({NAME: TYPE, ...})``

    The following extension types are also supported:

    - ``"json"`` -> ``PaJSONType()``
    - ``"pickle"`` -> ``PaPickleType()``
    - ``"ndarray<(item:)? TYPE>"`` -> ``PaNDArrayType(TYPE)``

    The following python type hints are supported:

    - ``Optional[TYPE] -> ``TYPE``
    - ``List[TYPE] -> ``pa.list_(TYPE)``
    - ``Dict[str, Any] -> ``PaJSONType()``

    .. _here: https://github.com/apache/arrow/blob/go/v10.0.0/python/pyarrow/types.pxi#L3159

    NOTE: nested types containing extension types are not supported
    """
    if isinstance(alias, str):
        alias = alias.strip()

    dtype = _get_primitive_dtype(alias)
    if dtype is not None:
        return dtype

    dtype = _get_generic_dtype(alias)
    if dtype is not None:
        return dtype

    dtype = _get_extension_dtype(alias)
    if dtype is not None:
        return dtype

    dtype = _get_nested_dtype(alias)
    if dtype is not None:
        return dtype

    raise ValueError(f"Unsupported dtype alias '{alias}'")


def _get_primitive_dtype(alias: DataType) -> Optional[pa.DataType]:
    try:
        return pa.lib.ensure_type(alias)
    except Exception:
        pass

    try:
        return pa.from_numpy_dtype(alias)
    except Exception:
        pass
    return None


def _get_generic_dtype(alias: Any) -> Optional[pa.DataType]:
    origin = get_origin(alias)
    if origin is None:
        return None
    args = get_args(alias)

    # optional, e.g. Optional[str]
    # unbox and recurse
    if origin is Union and len(args) == 2 and isinstance(None, args[1]):
        return get_dtype(args[0])

    # list type, e.g. List[str]
    # recurse
    if origin is list:
        dtype = get_dtype(args[0])
        return pa.list_(dtype)

    # generic record type Dict[str, ...]
    # assume json
    if origin is dict and len(args) >= 1 and args[0] is str:
        return PaJSONType()
    return None


def _get_extension_dtype(alias: DataType) -> Optional[pa.DataType]:
    if not isinstance(alias, str):
        return None

    alias = alias.lower()

    if alias == "json":
        return PaJSONType()

    if alias == "pickle":
        return PaPickleType()

    if alias.startswith("ndarray"):
        return _ndarray_from_string(alias)
    return None


def _get_nested_dtype(alias: DataType) -> Optional[pa.DataType]:
    if not isinstance(alias, str):
        return None

    dtype = _struct_from_string(alias)
    if dtype is not None:
        return dtype

    dtype = _list_from_string(alias)
    if dtype is not None:
        return dtype
    return None


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
