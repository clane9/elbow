# pylint: disable=redefined-outer-name
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from elbow import utils as ut

DATA_DIR = Path(__file__).parent / "data"


def test_lockopen(tmp_path: Path):
    fname = tmp_path / "file.txt"

    def _task(task_id: int):
        with ut.lockopen(fname, mode="a+") as f:
            t = time.time()
            lines = f.readlines()
            last_line = lines[-1] if lines else ""
            new_line = f"id={task_id}, t={t}"
            logging.debug(f"last line={last_line}\tnew line={new_line}")
            time.sleep(0.01)
            print(new_line, file=f)
        return task_id

    num_tasks = 10
    pool = ThreadPoolExecutor(4)
    for task_id in pool.map(_task, range(num_tasks)):
        logging.debug(f"Done {task_id}")

    with open(fname) as f:
        lines = f.readlines()
    assert len(lines) == num_tasks


def test_atomicopen(tmp_path: Path):
    fname = tmp_path / "file.txt"
    with ut.atomicopen(fname, "x") as f:
        logging.info(f"atomic temp file: {f.name}")
        print("blah", file=f)
        assert not fname.exists()
    assert fname.exists()
    assert not Path(f.name).exists()


def test_atomicopen_error(tmp_path: Path):
    fname = tmp_path / "file.txt"
    try:
        with ut.atomicopen(fname, "x") as f:
            raise RuntimeError
    except RuntimeError:
        pass
    assert not fname.exists()
    assert not Path(f.name).exists()


def test_atomicopen_mode():
    with pytest.raises(ValueError):
        with ut.atomicopen("file.txt", "r"):
            pass


@pytest.mark.parametrize(
    ("alias", "expected"),
    [("1GB", int(1e9)), ("1  gb", int(1e9)), ("7.23 KiB", 7403), (" 1 KB ", 1000)],
)
def test_parse_size(alias: str, expected: int):
    size = ut.parse_size(alias)
    assert size == expected


def test_parse_size_error():
    with pytest.raises(ValueError):
        ut.parse_size("10 PB")


@pytest.mark.parametrize(
    ("size", "expected", "expected_units"),
    [(23, 23.0, "B"), (2300, 2.3, "KB"), (23000000, 23.0, "MB")],
)
def test_detect_size_units(size: int, expected: float, expected_units: str):
    val, units = ut.detect_size_units(size)
    assert val == expected
    assert units == expected_units


if __name__ == "__main__":
    pytest.main([__file__])
