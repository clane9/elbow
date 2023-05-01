from typing import Any, Dict, Iterable, Optional, Union

import numpy as np
import pyarrow as pa
from pandas.api.extensions import register_extension_dtype
from pandas.core.arrays import PandasArray

from .base import PaExtensionArray, PaExtensionScalar, PaExtensionType, PdExtensionDtype

__all__ = [
    "PaNDArrayType",
    "PaNDArrayArray",
    "PdNDArrayDtype",
    "PdNDArrayArray",
]


class PaNDArrayType(PaExtensionType):
    """
    PyArrow ndarray extension type backed by a struct with fields:

        - data: flattened array data
        - shape: original array shape

    See `here <https://arrow.apache.org/docs/python/extending_types.html>`_ for more
    details on extension types.
    """

    def __init__(self, item_type: Optional[pa.DataType] = None):
        if item_type is None:
            item_type = pa.float32()
        fields = {
            "data": pa.list_(item_type),
            "shape": pa.list_(pa.int64()),
        }
        self.item_type = item_type
        super().__init__(pa.struct(fields), "ndarray")

    def __arrow_ext_serialize__(self):
        return str(self.item_type).encode()

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        alias = serialized.decode()
        item_type = pa.lib.ensure_type(alias)
        return cls(item_type)

    def __arrow_ext_scalar_class__(self):
        return PaExtensionScalar

    def __arrow_ext_class__(self):
        return PaNDArrayArray

    def to_pandas_dtype(self):
        return PdNDArrayDtype()

    def pack(self, value: Optional[np.ndarray]) -> Optional[Dict[str, Any]]:
        """
        Convert an array to a dict with ``"data"`` and ``"shape"`` fields, for pyarrow
        consumption.
        """
        if value is None:
            return value
        value = np.asarray(value)
        dtype = self.item_type.to_pandas_dtype()
        data = value.flatten().astype(dtype)
        return {"data": data, "shape": value.shape}

    def unpack(self, value: Union[pa.Scalar, Dict[str, Any]]) -> Optional[np.ndarray]:
        """
        Convert a pyarrow struct scalar or dict with ``"data"`` and ``"shape"`` fields
        back to a numpy array.
        """
        if value is None or pa.compute.is_null(value).as_py():
            return None
        data = value["data"]
        shape = value["shape"]
        if isinstance(value, pa.Scalar):
            data = data.values.to_numpy()
            shape = shape.as_py()
        data = np.asarray(data)
        return data.reshape(shape)

    def __str__(self) -> str:
        return f"ndarray<item: {self.item_type}>"


# Note that even though the registration uses float32, it still works for any
# item type.
# https://arrow.apache.org/docs/python/generated/pyarrow.register_extension_type.html#pyarrow.register_extension_type
pa.register_extension_type(PaNDArrayType())


class PaNDArrayArray(PaExtensionArray):
    """
    PyArrow ndarray array that unpacks and unflattens in python conversion
    (``array.to_pylist()``) and numpy conversion (``array.to_numpy()``).
    """

    def to_numpy(self, **kwargs):
        item_dtype = self.type.item_type.to_pandas_dtype()
        return _array_of_arrays(self.to_pylist(), item_dtype=item_dtype)

    @classmethod
    def from_sequence(
        cls,
        values: Iterable[np.ndarray],
        *,
        item_dtype: Any = None,
    ) -> "PaNDArrayArray":
        """
        Construct an array from a python iterable, first flattening and packing the
        ``values`` as structs.
        """
        if item_dtype is None:
            item_dtype = _infer_dtype(values)
        typ = PaNDArrayType(pa.from_numpy_dtype(item_dtype))
        return cls._from_sequence(values, typ)


@register_extension_dtype
class PdNDArrayDtype(PdExtensionDtype):
    """
    Pandas extension dtype for ndarrays supporting conversion to a PyArrow struct-backed
    extension type (``PaNDArrayType``).

    See `here
    <https://pandas.pydata.org/docs/development/extending.html#extension-types>`_ for
    more details on extension types.
    """

    name = "ndarray"

    @classmethod
    def construct_array_type(cls):
        """
        Return the array type associated with this dtype.
        """
        return PdNDArrayArray


class PdNDArrayArray(PandasArray):
    """
    Pandas extension array for ndarrays supporting conversion to a PyArrow struct-backed
    extension type (``PaNDArrayType``).
    """

    _typ = "extension"
    _dtype = PdNDArrayDtype()
    _internal_fill_value = None
    _str_na_value = None

    def __init__(
        self,
        values: Iterable[np.ndarray],
        *,
        copy: bool = False,
        item_dtype: Any = None,
    ):
        if not isinstance(values, np.ndarray):
            values = _array_of_arrays(values, item_dtype=item_dtype)
        if values.ndim != 1:
            raise ValueError("Only one-dimensional arrays supported")
        if copy:
            values = values.copy()
        super(PandasArray, self).__init__(values, PdNDArrayDtype())
        self.item_dtype = item_dtype

    @classmethod
    def _from_sequence(
        cls, scalars, *, dtype: Any = None, copy: bool = False
    ) -> "PdNDArrayArray":
        return PdNDArrayArray(scalars, copy=copy)

    def __arrow_array__(self, type: Optional[pa.DataType] = None) -> PaNDArrayArray:
        """
        Convert myself into a PyArrow array
        """
        return PaNDArrayArray.from_sequence(self._ndarray, item_dtype=self.item_dtype)


def _array_of_arrays(
    values: Iterable[np.ndarray], item_dtype: Any = None
) -> np.ndarray:
    """
    Construct a 1d array of numpy arrays.
    """
    if item_dtype is None:
        item_dtype = _infer_dtype(values)

    # Our goal is to construct a one-dimensional array of arrays. But numpy will
    # auto-stack the arrays into a single ndarray if the constituents happen to be all
    # the same shape. As a hack workaround, we initialize with an empty array, which we
    # remove below.
    values_ = [np.array([], dtype=item_dtype)]
    for v in values:
        v_ = np.asarray(v, dtype=item_dtype)
        values_.append(v_)
    array = np.asarray(values_, dtype=object)[1:]
    return array


def _infer_dtype(values: Iterable[np.ndarray]) -> np.dtype:
    """
    Infer the value dtype from a list of arrays.
    """
    for v in values:
        v_ = np.asarray(v)
        if v_.dtype != object:
            return v_.dtype
        if v_.size > 0:
            return np.dtype(type(v_.flatten()[0]))
    raise ValueError("Can't infer dtype from empty arrays of type object")
