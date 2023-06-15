"""
Generate a dataset of fake random images.
"""

from pathlib import Path
from shutil import rmtree

import numpy as np
from PIL import Image
from tqdm import tqdm

NUM_BATCHES = 16
BATCH_SIZE = 256
HEIGHT = 256
WIDTH = 256
SEED = 2023


rng = np.random.default_rng(SEED)

images_path = Path(__file__).parent / "images"
if images_path.exists():
    rmtree(images_path)

for batch_idx in tqdm(range(NUM_BATCHES)):
    batch_dir = images_path / f"{batch_idx:03d}"
    batch_dir.mkdir(parents=True)

    for img_idx in range(BATCH_SIZE):
        img_path = batch_dir / f"{img_idx:04d}.jpg"
        pixel_values = rng.integers(0, 255, size=(HEIGHT, WIDTH, 3), dtype=np.uint8)
        img = Image.fromarray(pixel_values)
        img.save(img_path)
