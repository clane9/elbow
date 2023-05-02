from pathlib import Path

import pytest

from elbow.filters import hash_partitioner


def test_hash_partitioner():
    partitioner = hash_partitioner(
        worker_id=1,
        num_workers=8,
        key=lambda path: Path(path).parent,
    )

    assert partitioner("A/a.txt")
    assert partitioner("A/b.txt")
    assert not partitioner("C/a.txt")


if __name__ == "__main__":
    pytest.main([__file__])
