import fcntl
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Tuple, Union

StrOrPath = Union[str, Path]


@contextmanager
def lockopen(path: StrOrPath, mode: str = "w", **kwargs):
    """
    Open a file with an exclusive lock.

    See also: https://github.com/dmfrey/FileLock/blob/master/filelock/filelock.py
    """
    file = open(path, mode, **kwargs)
    fcntl.flock(file.fileno(), fcntl.LOCK_EX)
    try:
        yield file
    finally:
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        file.close()


@contextmanager
def atomicopen(path: StrOrPath, mode: str = "w", **kwargs):
    """
    Open a file for "atomic" all-or-nothing writing. Only write modes are supported.
    """
    if mode[0] not in {"w", "x"}:
        raise ValueError(f"Only write modes supported; not '{mode}'")
    path = Path(path)
    file = tempfile.NamedTemporaryFile(
        mode=mode,
        dir=path.parent,
        prefix=".tmp-",
        suffix=path.suffix,
        delete=False,
        **kwargs,
    )
    try:
        yield file
    except Exception as exc:
        file.close()
        os.remove(file.name)
        raise exc
    else:
        file.close()
        os.replace(file.name, path)


def parse_size(size: str) -> int:
    """
    Parse a human readable size string like ``10MB`` to integer bytes.
    """
    units = {
        "B": 1,
        "KB": 10**3,
        "MB": 10**6,
        "GB": 10**9,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    units_lower = {k.lower(): v for k, v in units.items()}

    pattern = rf"([0-9.\s]+)({'|'.join(units_lower.keys())})"
    match = re.match(pattern, size, flags=re.IGNORECASE)
    if match is None:
        raise ValueError(
            f"Size {size} didn't match any of the following units:\n\t"
            + ", ".join(units.keys())
        )
    size = match.group(1)
    num = float(size)
    unit = match.group(2)
    bytesize = int(num * units_lower[unit.lower()])
    return bytesize


def detect_size_units(size: Union[int, float]) -> Tuple[float, str]:
    """
    Given ``size`` in bytes, find the best size unit and return ``size`` in those units.

    Example:
        >>> detect_size_units(2000)
        (2.0, 'KB')
    """
    if size < 1e3:
        return float(size), "B"
    elif size < 1e6:
        return size / 1e3, "KB"
    elif size < 1e9:
        return size / 1e6, "MB"
    else:
        return size / 1e9, "GB"
