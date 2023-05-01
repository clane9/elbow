"""
File metadata extractors.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from elbow.typing import StrOrPath

__all__ = ["FileMetadata", "file_meta"]


@dataclass
class FileMetadata:
    """
    Basic file metadata.
    """

    file_path: str
    # TODO: Will this type be inferred correctly?
    link_target: Optional[str]
    # TODO: Should this be a time-aware type?
    mod_time: Optional[float]


def file_meta(path: StrOrPath) -> FileMetadata:
    """
    File metadata extractor.
    """
    path = Path(path)
    target = str(path.resolve()) if path.is_symlink() else None
    mtime = path.stat().st_mtime if path.exists() else None
    return FileMetadata(
        file_path=str(path.absolute()), link_target=target, mod_time=mtime
    )
