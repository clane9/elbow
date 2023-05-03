from pathlib import Path

import pytest

from elbow.sources import crawldirs


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


def test_crawldirs(dummy_tree: Path):
    paths = list(crawldirs(root=dummy_tree, exclude=".*"))
    assert sorted(paths) == [
        dummy_tree / "A" / "b.txt",
        dummy_tree / "A" / "c.json",
        dummy_tree / "B" / "b" / "c.txt",
    ]


if __name__ == "__main__":
    pytest.main([__file__])
