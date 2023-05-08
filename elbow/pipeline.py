import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, no_type_check

import tqdm

from elbow.extractors import Extractor
from elbow.record import RecordLike, is_recordlike
from elbow.typing import StrOrPath

__all__ = ["ProcessCounts", "Pipeline"]


@dataclass
class ProcessCounts:
    total: int = 0
    success: int = 0
    record: int = 0
    error: int = 0


class Pipeline:
    """
    A streaming extract-load data pipeline with an optional max number of accepted
    failures.
    """

    def __init__(
        self,
        source: Iterable[StrOrPath],
        extract: Extractor,
        sink: Callable[[RecordLike], None],
        max_failures: Optional[int] = 0,
        progress: bool = True,
    ):
        self.max_failures = max_failures
        self.progress = progress

        self._source = source
        self._extract = extract
        self._sink = sink

    def run(self) -> ProcessCounts:
        """
        Run the pipeline.
        """
        # TODO:
        #   - setup/teardown?
        #   - what if this is called multiple times?
        counts = ProcessCounts()

        if self.progress:
            iterator = tqdm.tqdm(self._source)
        else:
            iterator = _null_progress(self._source)

        with iterator as it:
            for path in it:
                counts.total += 1
                try:
                    stream = _extract_stream(path, self._extract)
                    for rec in stream:
                        if rec is None:
                            continue
                        self._sink(rec)
                        counts.record += 1
                    counts.success += 1

                except Exception as exc:
                    logging.warning("Failed to process %s", path, exc_info=exc)
                    counts.error += 1
                    if (
                        self.max_failures is not None
                        and counts.error > self.max_failures >= 0
                    ):
                        raise RuntimeError("Too many errors in pipeline") from exc

                if isinstance(it, tqdm.tqdm):
                    it.set_postfix(
                        ordered_dict={
                            "tot": counts.total,
                            "good": counts.success,
                            "rec": counts.record,
                            "err": counts.error,
                        }
                    )

        return counts


@no_type_check
def _extract_stream(
    path: StrOrPath, extract: Extractor
) -> Iterable[Optional[RecordLike]]:
    stream = extract(path)
    # TODO: is_recordlike isn't interpreted by the type-checker as narrowing the type to
    # RecordLike. Is there a way to fix this?
    if stream is None or is_recordlike(stream):
        stream = [stream]
    return stream


@contextmanager
def _null_progress(source: Iterable[StrOrPath]):
    yield source
