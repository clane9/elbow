from typing import Any, Hashable, Iterable, Tuple, Type, Union

import numpy as np
import pyarrow as pa
from pandas.api.extensions import ExtensionArray, ExtensionDtype

__all__ = [
    "PaExtensionType",
    "PaExtensionScalar",
    "PaExtensionArray",
    "PdExtensionDtype",
]


class PaExtensionType(pa.ExtensionType):
    """
    A shallow sub-class of ``pyarrow.ExtensionType`` that adds methods to be
    implemented:

        - ``pack()``
        - ``unpack()``
    """

    def pack(self, value: Any) -> Any:
        """
        Pack an object so it can be directly consumed by pyarrow as this type
        """
        raise NotImplementedError

    def unpack(self, value: Any) -> Any:
        """
        Unpack a pyarrow scalar back to a python object.
        """
        raise NotImplementedError

    def __key(self) -> Tuple[Hashable, ...]:
        return (self.__class__.__name__, self.__arrow_ext_serialize__())

    def __hash__(self) -> int:
        return hash(self.__key())


class PaExtensionScalar(pa.ExtensionScalar):
    """
    A shallow sub-class of ``pyarrow.ExtensionScalar`` that adds a default
    implementation for ``as_py()``.
    """

    def as_py(self) -> Any:
        val = self.value
        if val is not None:
            val = self.type.unpack(val)
        return val


# TODO: How useful are the ExtensionArray and ExtensionScalar classes really?
class PaExtensionArray(pa.ExtensionArray):
    """
    A shallow sub-class of ``pyarrow.ExtensionArray`` that adds methods to be
    implemented:

        - ``from_sequence()``

    and a default implementation for ``to_numpy()``.
    """

    def to_numpy(self, **kwargs):
        """
        Convert extension array to a numpy ndarray, via a python list.

        .. warning::
            Converting via a python list is very inefficient.
        """
        return np.array(self.to_pylist(), dtype=object)

    @classmethod
    def from_sequence(cls, values: Iterable[Any]) -> "PaExtensionArray":
        """
        Construct an array from a python sequence.
        """
        raise NotImplementedError

    @classmethod
    def _from_sequence(
        cls, values: Iterable[Any], typ: PaExtensionType
    ) -> "PaExtensionArray":
        """
        Construct an array from a python sequence, first packing each of the ``values``
        according to the extension type ``typ``.
        """
        storage = [typ.pack(val) for val in values]
        storage = pa.array(storage, type=typ.storage_type)
        array = cls.from_storage(typ, storage)
        return array


class PdExtensionDtype(ExtensionDtype):
    """
    A shallow sub-class of ``pandas.api.extensions.ExtensionDtype`` that adds a default
    implementation for ``__from_arrow__()`` .

    See `here
    <https://pandas.pydata.org/docs/development/extending.html#extension-types>`_ for
    more details on extension types.
    """

    # Concrete sub-classes should define ``name``.
    # name = None

    # These can be overridden.
    type = object
    kind = "O"
    base = np.dtype("O")
    na_value = None

    @classmethod
    def construct_array_type(cls) -> Type[ExtensionArray]:
        """
        Return the array type associated with this dtype.
        """
        raise NotImplementedError

    def __from_arrow__(self, array: Union[pa.Array, pa.ChunkedArray]) -> ExtensionArray:
        if isinstance(array, pa.ChunkedArray):
            values = np.concatenate([chunk.to_numpy() for chunk in array.iterchunks()])
        else:
            values = array.to_numpy()
        array_typ = self.construct_array_type()
        return array_typ._from_sequence(values)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
