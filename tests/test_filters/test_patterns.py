import pytest

from elbow.filters import glob_filter, regex_filter


@pytest.mark.parametrize(
    "path,expected",
    [
        ("a.txt", True),
        ("abc.txt", True),
        ("A/b.txt", True),
        ("a.txt.gz", False),
        ("a.json", False),
        ("A/b/c.json", True),
        ("A/b/c/d.json", True),
        ("A/b.json", False),
    ],
)
@pytest.mark.parametrize("exclude", [False, True])
def test_glob_filter(path: str, expected: bool, exclude: bool):
    pattern = ["*.txt", "A/**/*.json"]
    filter = glob_filter(pattern, exclude=exclude)
    if exclude:
        expected = not expected
    assert filter(path) == expected


def test_regex_filter():
    # No need to extensively test regex..
    pattern = r".*\.json"
    filter = regex_filter(pattern)
    assert filter("A/b/c.json")


if __name__ == "__main__":
    pytest.main([__file__])
