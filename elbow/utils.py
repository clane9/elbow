import logging
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

StrOrPath = Union[str, Path]


class RepetitiveFilter(logging.Filter):
    """
    Suppress similar log messages after a number of repeats.
    """

    def __init__(self, max_repeats: int = 5):
        self.max_repeats = max_repeats

        self._counts: Dict[Tuple[str, int], int] = {}

    def filter(self, record: logging.LogRecord):
        key = record.pathname, record.lineno
        count = self._counts.get(key, 0)
        if count == self.max_repeats:
            record.msg += " [future messages suppressed]"
        self._counts[key] = count + 1
        return count <= self.max_repeats


def setup_logging(
    level: Optional[Union[int, str]] = None,
    max_repeats: Optional[int] = 5,
    log_path: Optional[StrOrPath] = None,
):
    """
    Setup root logger.
    """
    fmt = "[%(levelname)s %(asctime)s]: " "%(message)s"
    formatter = logging.Formatter(fmt, datefmt="%y-%m-%d %H:%M:%S")

    logger = logging.getLogger()
    if level is not None:
        logger.setLevel(level)

    # clean up any pre-existing filters and handlers
    for f in logger.filters:
        logger.removeFilter(f)
    for h in logger.handlers:
        logger.removeHandler(h)

    if max_repeats:
        logger.addFilter(RepetitiveFilter(max_repeats=max_repeats))

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logger.level)
    logger.addHandler(stream_handler)

    if log_path is not None:
        file_handler = logging.FileHandler(log_path, mode="a")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logger.level)
        logger.addHandler(file_handler)

    # Redefining the root logger is not strictly best practice.
    # https://stackoverflow.com/a/7430495
    # But I want the convenience to just call e.g. `logging.info()`.
    logging.root = logger  # type: ignore


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
        # don't bother replacing if we didn't end up writing anything
        if os.stat(file.name).st_size > 0:
            os.replace(file.name, path)
        else:
            os.remove(file.name)


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


def cpu_count() -> int:
    """
    Get the number of available CPUs.
    """
    if "SLURM_CPUS_ON_NODE" in os.environ:
        count = int(os.environ["SLURM_CPUS_ON_NODE"])
    else:
        count = os.cpu_count() or 1
    return count
