import fnmatch
import re
from pathlib import Path
from typing import List, Union

from elbow.typing import Filter, StrOrPath

__all__ = ["glob_filter", "regex_filter"]


def glob_filter(pattern: Union[str, List[str]], exclude: bool = False) -> Filter:
    """
    Generate a filter based on glob pattern(s). Patterns containing '/' match against
    the full posix path, and otherwise just the name. Set `exclude` to exclude rather
    than include matches.
    """
    if isinstance(pattern, str):
        pattern = [pattern]
    include = not exclude

    def _filter(path: StrOrPath):
        path = Path(path)
        path_posix = path.as_posix()
        path_name = path.name

        for pat in pattern:
            query = path_posix if "/" in pat else path_name
            if fnmatch.fnmatch(query, pat):
                return include
        return not include

    return _filter


def regex_filter(pattern: str) -> Filter:
    """
    Generate a filter based on a regex pattern.
    """
    compiled = re.compile(pattern)

    def _filter(path: StrOrPath):
        return compiled.match(str(path)) is not None

    return _filter
