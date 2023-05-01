from typing import Iterable, Optional, Union

from typing_extensions import Protocol, runtime_checkable

from elbow.record import RecordLike
from elbow.typing import StrOrPath


# NOTE: runtime isinstance check only seems to verify that the object is callable, not
# the rest of the signature.
@runtime_checkable
class Extractor(Protocol):
    """
    An abstract extractor interface. To satisfy the interface, an extractor should take
    an input path and return an optional RecordLike, or iterable thereof.
    """

    def __call__(
        self, path: StrOrPath
    ) -> Union[Optional[RecordLike], Iterable[Optional[RecordLike]]]:
        ...
