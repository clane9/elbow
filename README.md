# 💪 Elbow
[![Build](https://github.com/cmi-dair/elbow/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/cmi-dair/elbow/actions/workflows/ci.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/cmi-dair/elbow/branch/main/graph/badge.svg?token=22HWWFWPW5)](https://codecov.io/gh/cmi-dair/elbow)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Elbow is a lightweight and scalable library for getting diverse data out of specialized formats and into common tabular data formats for downstream analytics.

## Example

Extract image metadata and pixel values from all JPEG image files under the current directory and save as a [Parquet](https://parquet.apache.org/) dataset.

```python
import numpy as np
import pandas as pd
from PIL import Image

from elbow.builders import build_parquet

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

build_parquet(
    source="**/*.jpg",
    extract=extract_image,
    output="images.pqds/",
    workers=8,
)

df = pd.read_parquet("images.pqds")
```

For a complete example, see [here](example/).

## Installation

```
pip install elbow
```

The current development version can be installed with

```
pip install git+https://github.com/cmi-dair/elbow.git
```

## Related projects

There are many other high quality projects for extracting, loading, and transforming data. Some alternative projects focused on somewhat different use cases are:

- [AirByte](https://github.com/airbytehq/airbyte)
- [Meltano](https://github.com/meltano/meltano)
- [Singer](https://github.com/singer-io/getting-started)
- [Mage](https://github.com/mage-ai/mage-ai)
- [Orchest](https://github.com/orchest/orchest)
- [Streamz](https://github.com/python-streamz/streamz)
- [🤗 Datasets](https://github.com/huggingface/datasets)

## Contributing

We welcome contributions of any kind! If you'd like to contribute, please feel free to start a conversation in our [issues](https://github.com/cmi-dair/elbow/issues).
