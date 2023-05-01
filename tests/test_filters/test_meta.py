from pathlib import Path
from typing import List

import pyarrow.parquet as pq
import pytest

from elbow.extractors import file_meta
from elbow.filters import FileModifiedIndex
from elbow.record import RecordBatch


@pytest.fixture
def dummy_files(tmp_path: Path) -> List[Path]:
    paths = []
    for name in ["a.txt", "b.txt"]:
        path = tmp_path / name
        path.touch()
        paths.append(path)
    return paths


@pytest.fixture
def file_index_parquet(tmp_path: Path, dummy_files: List[Path]) -> Path:
    path = tmp_path / "index.parquet"

    index = RecordBatch()
    for path in dummy_files:
        metadata = file_meta(path)
        index.append(metadata)

    index = index.to_arrow()
    pq.write_table(index, path)
    return path


def test_file_modified_index(
    tmp_path: Path,
    dummy_files: List[Path],
    file_index_parquet: Path,
):
    index = FileModifiedIndex.from_parquet(file_index_parquet)

    # Old and unchanged
    assert not index(dummy_files[0])

    # Changed file
    dummy_files[1].touch()
    assert index(dummy_files[1])

    # New file
    new_file = tmp_path / "new.txt"
    new_file.touch()
    assert index(new_file)

    # Non-existent file
    nonexist_file = tmp_path / "nonexist.txt"
    assert not index(nonexist_file)


if __name__ == "__main__":
    pytest.main([__file__])
