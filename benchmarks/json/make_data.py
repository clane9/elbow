from pathlib import Path
from shutil import rmtree

from tests.utils_for_tests import random_jsonl_batch

NUM_BATCHES = 64
BATCH_SIZE = 256
SEED = 2022


dataset_path = Path(__file__).parent / "data"
if dataset_path.exists():
    rmtree(dataset_path)
dataset_path.mkdir()

for ii in range(NUM_BATCHES):
    random_jsonl_batch(dataset_path, BATCH_SIZE, seed=(SEED + ii))
