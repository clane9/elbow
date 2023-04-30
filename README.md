# Elbow

Elbow is a library for extracting data from a bunch of files and loading into table (and that's it).

## Examples

```python
import json

from elbow import load_table, load_parquet


# Extract records from JSON-lines
def extract(path):
    with open(path) as f:
        for line in path:
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
pip install git+https://github.com/clane9/elbow
```
