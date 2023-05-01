import pickle
from typing import Any, Iterable, Optional, Union

import numpy as np
import pyarrow as pa
from pandas.api.extensions import register_extension_dtype
from pandas.core.arrays import PandasArray

from .base import PaExtensionArray, PaExtensionScalar, PaExtensionType, PdExtensionDtype

__all__ = [
    "PaPickleType",
    "PaPickleArray",
    "PdPickleDtype",
    "PdPickleArray",
]


class PaPickleType(PaExtensionType):
    """
    PyArrow binary extension type for holding arbitrary pickled objects.
    """

    def __init__(self):
        super().__init__(pa.binary(), "pickle")

    def __arrow_ext_serialize__(self):
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return PaPickleType()

    def __arrow_ext_scalar_class__(self):
        return PaExtensionScalar

    def __arrow_ext_class__(self):
        return PaPickleArray

    def to_pandas_dtype(self):
        return PdPickleDtype()

    def pack(self, value: Any) -> Optional[bytes]:
        """
        Pack an object by serializing with pickle
        """
        if value is None:
            return value
        # TODO: could imagine dropping in different pickle implementations here, perhaps
        # using a parameter on the type to select which one. This way, the data that
        # needs it can use more portable pickling.
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    def unpack(self, value: Union[pa.Scalar, Optional[bytes]]) -> Optional[Any]:
        """
        Unpack a binary pyarrow scalar back to a python object by deserializing with
        pickle.
        """
        # NOTE: pyarrow False boolean scalars are Truthy
        # https://github.com/apache/arrow/issues/34987
        if value is None or pa.compute.is_null(value).as_py():
            return None
        if isinstance(value, pa.Scalar):
            value = value.as_py()
        return pickle.loads(value)

    def __str__(self) -> str:
        return "pickle"


pa.register_extension_type(PaPickleType())


class PaPickleArray(PaExtensionArray):
    """
    PyArrow pickle array that deserializes with pickle in python conversion
    (``array.to_pylist()``) and numpy conversion (``array.to_numpy()``).
    """

    @classmethod
    def from_sequence(cls, values: Iterable[Any]) -> "PaPickleArray":
        """
        Construct an array from a python sequence, first serializing the ``values``
        using pickle.
        """
        return cls._from_sequence(values, PaPickleType())


@register_extension_dtype
class PdPickleDtype(PdExtensionDtype):
    """
    Pandas extension dtype for arbitrary objects supporting conversion to a PyArrow
    binary extension type (``PaPickleType``) via pickle serialization.
    """

    name = "pickle"

    @classmethod
    def construct_array_type(cls):
        """
        Return the array type associated with this dtype.
        """
        return PdPickleArray


class PdPickleArray(PandasArray):
    """
    Pandas extension array for arbitrary objects supporting conversion to PyArrow (via
    pickle serialization).
    """

    # pandas tries to "unbox" PandasArrays to plain numpy arrays wherever possible.
    # (Searching for `ABCPandasArray` gives more context why and how.) This causes
    # issues when converting from pyarrow. So we reset `_typ` to prevent this.
    _typ = "extension"
    _dtype = PdPickleDtype()
    _internal_fill_value = None
    _str_na_value = None

    def __init__(self, values: np.ndarray, copy: bool = False):
        values = np.asarray(values, dtype=object)
        if values.ndim != 1:
            raise ValueError("Only one-dimensional arrays supported")
        if copy:
            values = values.copy()
        super(PandasArray, self).__init__(values, PdPickleDtype())

    @classmethod
    def _from_sequence(
        cls, scalars, *, dtype: Any = None, copy: bool = False
    ) -> "PdPickleArray":
        return PdPickleArray(scalars, copy=copy)

    def __arrow_array__(self, type: Optional[pa.DataType] = None) -> PaPickleArray:
        return PaPickleArray.from_sequence(self._ndarray)
