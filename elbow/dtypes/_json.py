import json
from typing import Any, Iterable, Optional, Union

import numpy as np
import pyarrow as pa
from pandas.api.extensions import register_extension_dtype
from pandas.core.arrays import PandasArray

from .base import PaExtensionArray, PaExtensionScalar, PaExtensionType, PdExtensionDtype

__all__ = [
    "PaJSONType",
    "PaJSONArray",
    "PdJSONDtype",
    "PdJSONArray",
]


class PaJSONType(PaExtensionType):
    """
    PyArrow extension type for holding JSON-encoded objects.
    """

    def __init__(self):
        super().__init__(pa.string(), "json")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return PaJSONType()

    def __arrow_ext_scalar_class__(self):
        return PaExtensionScalar

    def __arrow_ext_class__(self):
        return PaJSONArray

    def to_pandas_dtype(self):
        return PdJSONDtype()

    def pack(self, value: Any) -> Optional[str]:
        """
        Pack an object by serializing with json
        """
        if value is None:
            return value
        return json.dumps(value)

    def unpack(self, value: Union[pa.Scalar, Optional[str]]) -> Optional[Any]:
        """
        Unpack a string pyarrow scalar back to a python object by deserializing with
        json.
        """
        if value is None or pa.compute.is_null(value).as_py():
            return None
        if isinstance(value, pa.Scalar):
            value = value.as_py()
        return json.loads(value)

    def __str__(self) -> str:
        return "json"


pa.register_extension_type(PaJSONType())


class PaJSONArray(PaExtensionArray):
    """
    PyArrow JSON array that deserializes in python conversion (``array.to_pylist()``)
    and numpy conversion (``array.to_numpy()``).
    """

    @classmethod
    def from_sequence(cls, values: Iterable[Any]) -> "PaJSONArray":
        """
        Construct an array from a python sequence, first serializing the ``values``
        using json.
        """
        return cls._from_sequence(values, PaJSONType())


@register_extension_dtype
class PdJSONDtype(PdExtensionDtype):
    """
    Pandas extension dtype for arbitrary objects supporting conversion to a PyArrow
    string extension type (``PaJSONType``) via json serialization.
    """

    name = "json"

    @classmethod
    def construct_array_type(cls):
        """
        Return the array type associated with this dtype.
        """
        return PdJSONArray


class PdJSONArray(PandasArray):
    """
    Pandas extension array for JSON objects supporting conversion to PyArrow.
    """

    _typ = "extension"
    _dtype = PdJSONDtype()
    _internal_fill_value = None
    _str_na_value = None

    def __init__(self, values: np.ndarray, copy: bool = False):
        values = np.asarray(values, dtype=object)
        if values.ndim != 1:
            raise ValueError("Only one-dimensional arrays supported")
        if copy:
            values = values.copy()
        super(PandasArray, self).__init__(values, PdJSONDtype())

    @classmethod
    def _from_sequence(
        cls, scalars, *, dtype: Any = None, copy: bool = False
    ) -> "PdJSONArray":
        return PdJSONArray(scalars, copy=copy)

    def __arrow_array__(self, type: Optional[pa.DataType] = None) -> PaJSONArray:
        return PaJSONArray.from_sequence(self._ndarray)
