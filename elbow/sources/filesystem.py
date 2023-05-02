import fnmatch
import os
from glob import glob
from pathlib import Path
from typing import Generator, List, Optional, Union

from elbow.typing import StrOrPath

__all__ = ["crawldirs"]


# TODO: Do we need this when we have iglob? I guess the only advantage is the option to
# exclude dirs from the search?


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

    for entry in root:
        for subdir, dirnames, fnames in os.walk(entry, followlinks=followlinks):
            if exclude:
                _remove_exclude(subdir, dirnames, exclude)

            for fname in fnames:
                yield Path(subdir) / fname


def _remove_exclude(root: StrOrPath, names: List[str], exclude: List[str]) -> None:
    """
    Remove names matching patterns in exclude in place.
    """
    root = Path(root)

    drop_indices = []
    for pat in exclude:
        full_path_query = "/" in pat
        for ind, name in enumerate(names):
            query = (root / name).as_posix() if full_path_query else name
            if fnmatch.fnmatch(query, pat):
                drop_indices.append(ind)
                break

    for ii, ind in enumerate(drop_indices):
        names.pop(ind - ii)
