# Elbow

Elbow is a library for extracting data from a bunch of files and loading into a table (and that's it).

## Examples

```python
import json

from elbow import load_table, load_parquet

# Extract records from JSON-lines
def extract(path):
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            yield record

# Load as a pandas dataframe
df = load_table(
    pattern="**/*.json",
    extract=extract,
)

# Load as a parquet dataset (in parallel)
dset = load_parquet(
    pattern="**/*.json",
    extract=extract,
    where="dset.parquet",
    workers=8,
)
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
