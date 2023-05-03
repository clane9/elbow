import fnmatch
import os
from glob import glob
from pathlib import Path
from typing import Generator, List, Optional, Union

from elbow.typing import StrOrPath

__all__ = ["crawldirs"]


# TODO: Do we need this when we have iglob? I guess the only advantage is the option to
# exclude dirs from the search?


class _Pattern:
    def __init__(self, pat: str):
        self.pat = pat
        self.full_path = "/" in pat


def crawldirs(
    root: Union[StrOrPath, List[StrOrPath]],
    exclude: Optional[Union[str, List[str]]] = None,
    followlinks: bool = False,
) -> Generator[Path, None, None]:
    """
    Crawl one or more directories and generate a stream of paths.

    Args:
        root: one or more directory paths or glob patterns to crawl.
        exclude: one or more glob patterns for sub-directory names to skip over.
        followlinks: whether to follow symbolic links.

    Yields:
        Crawled file paths.
    """
    if isinstance(root, (str, Path)):
        root = [root]
    # Expand root directories with glob.
    root = [entry for pat in root for entry in sorted(glob(str(pat)))]

    if exclude is None:
        exclude = []
    elif isinstance(exclude, str):
        exclude = [exclude]
    exclude_pats = [_Pattern(pat) for pat in exclude]

    for entry in root:
        for subdir, dirnames, fnames in os.walk(entry, followlinks=followlinks):
            if exclude:
                _remove_exclude(subdir, dirnames, exclude_pats)
            for fname in fnames:
                yield Path(subdir) / fname


def _remove_exclude(root: StrOrPath, names: List[str], exclude: List[_Pattern]) -> None:
    """
    Remove names matching patterns in exclude in place.
    """
    root = Path(root)
    names_copy = names.copy()
    num_names = len(names)
    for ii, name in enumerate(reversed(names_copy)):
        for pat in exclude:
            query = (root / name).as_posix() if pat.full_path else name
            if fnmatch.fnmatch(query, pat.pat):
                names.pop(num_names - ii - 1)
                break
