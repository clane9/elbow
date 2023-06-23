import argparse
from pathlib import Path

import numpy as np
from PIL import Image

from elbow.builders import build_parquet
from elbow.utils import setup_logging


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("root", metavar="ROOT", type=Path, help="Path to images")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Path to output parquet dataset directory. (default: images.pqds)",
        default="images.pqds",
    )
    parser.add_argument(
        "-w",
        "--workers",
        metavar="COUNT",
        type=int,
        help="Number of worker processes. Setting to -1 runs as many processes as "
        "there are cores available. (default: 1)",
        default=1,
    )
    parser.add_argument(
        "--worker_id",
        metavar="RANK",
        type=int,
        help="Optional worker ID to use when scheduling parallel tasks externally."
        " (default: None)",
        default=None,
    )
    parser.add_argument(
        "-x",
        "--overwrite",
        help="Overwrite previous dataset.",
        action="store_true",
    )
    parser.add_argument("-v", "--verbose", help="Verbose logging.", action="store_true")

    args = parser.parse_args()
    setup_logging("INFO" if args.verbose else "ERROR")

    build_parquet(
        source=str(args.root / "**" / "*.jpg"),
        extract=extract_image,
        output=args.output,
        overwrite=args.overwrite,
        workers=args.workers,
        worker_id=args.worker_id,
    )


def extract_image(path: str):
    img = Image.open(path)
    width, height = img.size
    pixel_values = np.asarray(img)
    return {
        "path": path,
        "width": width,
        "height": height,
        "pixel_values": pixel_values,
    }


if __name__ == "__main__":
    main()
