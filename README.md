# 💪 Elbow
[![Build](https://github.com/cmi-dair/elbow/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/cmi-dair/elbow/actions/workflows/ci.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/clane9/elbow/branch/main/graph/badge.svg?token=22HWWFWPW5)](https://codecov.io/gh/clane9/elbow)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Elbow is a library for extracting data from a bunch of files and loading into a table (and that's it).

## Examples

```python
import json

import pandas as pd
from elbow import build_table, build_parquet

# Extract records from JSON-lines
def extract(path):
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            yield record

# Load as a pandas dataframe
df = build_table(
    source="**/*.json",
    extract=extract,
)

# Load as a parquet dataset (in parallel)
build_parquet(
    source="**/*.json",
    extract=extract,
    where="dset.pqds/",
    workers=8,
)

df = pd.read_parquet("dset.pqds")
```

## Installation

A pre-release version can be installed with

```
pip install elbow
```

## Other (better) projects

- [AirByte](https://github.com/airbytehq/airbyte)
- [Meltano](https://github.com/meltano/meltano)
- [Singer](https://github.com/singer-io/getting-started)
- [Mage](https://github.com/mage-ai/mage-ai)
- [Orchest](https://github.com/orchest/orchest)
- [Streamz](https://github.com/python-streamz/streamz)
