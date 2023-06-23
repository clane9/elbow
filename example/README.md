# Elbow example

In this example we'll extract metadata and pixel values from a bunch of fake random images and save as a [Parquet](https://parquet.apache.org/) dataset.

## Install packages

Install some packages if you don't have them already

```sh
pip install numpy Pillow tqdm
```

## Generating fake images

Run the `generate_images.py` script to generate a dataset of fake random images in [images/](images/).

```sh
python generate_images.py
```

## Extract images to Parquet

The code to run the example is in [example.py](example.py). You can run it as a command-line program. Check out the help message with.

```sh
python example.py -h
```

In the simplest case, you can build the dataset serially

```sh
python example.py -o images.pqds/ images/
```

The output Parquet dataset `images.pqds/` should contain a single Parquet file.

```
$ ls -lh images.pqds/
total 1607248
-rw-------  1 clane  staff   770M Jun 15 17:08 part-20230615170755-0000-of-0001.parquet
```

Then you can load the Parquet dataset in python.

```python
import pandas as pd

# Register extension arrow dtypes, e.g. for ndarrays
import elbow.dtypes

df = pd.read_parquet("images.pqds")
```

## Parallelized extraction

To extract in parallel across four workers, you can run

```sh
python example.py -o images.pqds/ --workers 4 --overwrite images/
```

In the output directory, you'll now see one Parquet "partition" file per worker

```
$ ls -lh images.pqds/
total 1644864
-rw-------  1 clane  staff   202M Jun 15 17:08 part-20230615170848-0000-of-0004.parquet
-rw-------  1 clane  staff   186M Jun 15 17:08 part-20230615170848-0001-of-0004.parquet
-rw-------  1 clane  staff   188M Jun 15 17:08 part-20230615170848-0002-of-0004.parquet
-rw-------  1 clane  staff   194M Jun 15 17:08 part-20230615170848-0003-of-0004.parquet
```

Alternatively, you can generate each partition independently by calling the script with `--worker_id`. This can be useful in HPC environments where you want to schedule many extraction tasks in parallel through a scheduler like [SLURM](https://slurm.schedmd.com/documentation.html).

```sh
rm -r images.pqds

for worker_id in {0..3}; do
  python example.py -o images.pqds/ --worker_id $worker_id --workers 4 images/ &
done
```
