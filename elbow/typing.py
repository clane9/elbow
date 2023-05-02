from pathlib import Path
from typing import Callable, ClassVar, Dict, Union

from typing_extensions import Protocol, runtime_checkable

StrOrPath = Union[str, Path]
Filter = Callable[[StrOrPath], bool]


# https://stackoverflow.com/a/55240861
@runtime_checkable
class Dataclass(Protocol):
    __dataclass_fields__: ClassVar[Dict]
