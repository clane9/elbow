from pathlib import Path

import pytest

from elbow.sources.filesystem import Crawler


@pytest.fixture
def dummy_tree(tmp_path: Path) -> Path:
    (tmp_path / "A").mkdir()
    (tmp_path / "B" / "b").mkdir(parents=True)
    (tmp_path / ".skip").mkdir()
    (tmp_path / ".skip2").mkdir()

    (tmp_path / "A" / "b.txt").touch()
    (tmp_path / "A" / "c.json").touch()
    (tmp_path / "B" / "b" / "c.txt").touch()
    (tmp_path / ".skip" / "skip.txt").touch()
    (tmp_path / ".skip2" / "skip2.txt").touch()
    return tmp_path


def test_crawler(dummy_tree: Path):
    paths = Crawler(
        root=dummy_tree,
        include=["*.txt", "*.json"],
        exclude=["b.txt"],
        skip=[".*"],
        files_only=True,
    )
    assert sorted(list(paths)) == [
        dummy_tree / "A" / "c.json",
        dummy_tree / "B" / "b" / "c.txt",
    ]


if __name__ == "__main__":
    pytest.main([__file__])
