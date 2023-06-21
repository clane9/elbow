import fnmatch
import os
from pathlib import Path
from typing import Generator, List, Optional, Union

from elbow.typing import StrOrPath

__all__ = ["crawldir"]


def crawldir(
    root: StrOrPath,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    skip: Optional[List[str]] = None,
    files_only: bool = False,
    dirs_only: bool = False,
    follow_links: bool = False,
) -> Generator[Path, None, None]:
    """
    Crawl a directory and generate a stream of file and directory paths.

    Args:
        root: root directory to crawl
        include: include results that match any of these patterns
        exclude: exclude results that match any of these patterns
        skip: one or more glob patterns for sub-directory names to skip crawling
        files_only: only return file paths
        dirs_only: only return directory paths
        follow_links: whether to follow symbolic links

    Yields:
        Crawled file paths.
    """
    if files_only and dirs_only:
        raise ValueError("Can't specify both files_only and dirs_only")

    include = _tolist(include)
    exclude = _tolist(exclude)
    skip = _tolist(skip)

    for subdir, dirnames, fnames in os.walk(root, followlinks=follow_links):
        names = []
        if not files_only:
            names.extend(dirnames)
        if not dirs_only:
            names.extend(fnames)

        names = _filter_include(names, include)
        names = _filter_exclude(names, exclude)

        subpath = Path(subdir)
        for name in names:
            yield subpath / name

        if skip:
            _remove_skip(subdir, dirnames, skip)


def _tolist(val: Optional[Union[str, List[str]]]) -> List[str]:
    if val is None:
        return []
    if isinstance(val, str):
        return [val]
    return val


def _remove_skip(root: StrOrPath, names: List[str], skip: List[str]) -> None:
    """
    Remove names matching patterns in skip in place.
    """
    root = Path(root)
    num_names = len(names)
    for ii in range(num_names - 1, -1, -1):
        name = names[ii]
        for pat in skip:
            if fnmatch.fnmatch(name, pat):
                names.pop(ii)
                break


def _filter_include(names: List[str], include: List[str]):
    """
    Keep names that match any pattern.
    """
    if not include:
        return names

    filtered = []
    for pat in include:
        filtered.extend(fnmatch.filter(names, pat))
    return filtered


def _filter_exclude(names: List[str], exclude: List[str]):
    """
    Drop names that match any pattern.
    """
    if not exclude:
        return names

    matches = set()
    for pat in exclude:
        matches.update(fnmatch.filter(names, pat))

    filtered = [name for name in names if name not in matches]
    return filtered
