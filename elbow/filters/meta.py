from pathlib import Path
from typing import Dict

import pandas as pd

from elbow.typing import StrOrPath

__all__ = ["FileModifiedIndex"]


class FileModifiedIndex:
    """
    An index mapping absolute file paths to modified times, supporting filtering for new
    and changed files.
    """

    def __init__(self, index: Dict[str, float]):
        self._index = index

    @classmethod
    def from_df(
        cls,
        df: pd.DataFrame,
        path_column: str = "file_path",
        mtime_column: str = "mod_time",
    ):
        """
        Initialize index from a dataframe with columns for the file path and modified
        time.
        """
        df = df[[path_column, mtime_column]]
        df = df.set_index(path_column)
        index = df[mtime_column].to_dict()
        return cls(index)

    @classmethod
    def from_parquet(
        cls,
        path: StrOrPath,
        path_column: str = "file_path",
        mtime_column: str = "mod_time",
    ):
        """
        Initialize index from a parquet file or directory of parquet files.
        """
        df = pd.read_parquet(path, columns=[path_column, mtime_column])
        return cls.from_df(df, path_column=path_column, mtime_column=mtime_column)

    def filter(self, path: StrOrPath) -> bool:
        """
        Test whether a path is new or has been modified since it was indexed.
        """
        # NOTE: paths are assumed to be absolute but not resolved. See also
        # the file meta extractor.
        path = Path(path).absolute()
        if not path.exists():
            return False
        mtime = path.stat().st_mtime
        path = str(path)
        if path not in self._index:
            return True
        old_mtime = self._index[path]
        return mtime > old_mtime

    __call__ = filter
