import hashlib
from typing import Callable, Optional

from elbow.typing import Filter, StrOrPath

__all__ = ["hash_partitioner"]


def hash_partitioner(
    worker_id: int,
    num_workers: int = 1,
    key: Optional[Callable[[StrOrPath], str]] = None,
) -> Filter:
    """
    Generate a filter for consistently self-assigning paths to workers using hashing.
    Optionally, a `key` can be used to gain some control over how paths are grouped
    together.

    Example::

        # Input stream
        stream = ...

        # Group by parent directory
        def key(path):
            return str(Path(path).parent)

        partitioner = hash_partition(worker_id=0, num_workers=8, key=key)
        stream = filter(partitioner, stream)
    """
    if not 0 <= worker_id < num_workers:
        raise ValueError(
            f"Invalid worker_id {worker_id} and/or num_workers {num_workers}"
        )

    def _filter(path: StrOrPath):
        if num_workers == 1:
            return True

        val = str(path) if key is None else str(key(path))
        # TODO: is MD5 the best hash to use? I care about speed and consistency, not
        # security. A non-cryptographic hash such as MurmurHash or xxHash might be
        # better (thanks GPT-4)
        bucket = int(hashlib.md5(val.encode("utf-8")).hexdigest(), 16) % num_workers
        return bucket == worker_id

    return _filter
