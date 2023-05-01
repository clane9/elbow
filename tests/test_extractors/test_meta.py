import json
import os
from pathlib import Path

import pytest

from elbow.extractors import file_meta


@pytest.fixture
def json_path(tmp_path: Path) -> Path:
    json_path = tmp_path / "dummy.json"
    with json_path.open("w") as f:
        json.dump({"dummy": True}, f)
    return json_path


def test_file_meta(json_path: Path):
    metadata = file_meta(json_path)

    assert metadata.file_path == str(json_path.absolute())
    assert metadata.link_target is None
    assert metadata.mod_time > 1672549200

    link_path = json_path.parent / "link.json"
    os.symlink(json_path, link_path)

    metadata = file_meta(link_path)
    assert metadata.file_path == str(link_path.absolute())
    assert metadata.link_target == str(json_path.absolute())
    assert metadata.mod_time > 1672549200


if __name__ == "__main__":
    pytest.main([__file__])
