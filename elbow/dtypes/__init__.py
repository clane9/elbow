# TODO: Check that loading parquet files in different libraries works as expected:
#
#   [x] Dask. Seems to work fine out of the box. This produces a pandas dataframe with
#       the converted extension type::
#
#           ddf.read_parquet("table.parquet", engine="pyarrow").compute()
#
#   [ ] DuckDB. Doesn't seem work right away. Extension type columns are unboxed to the
#       storage type. Conversion to pandas doesn't automatically transform the data::
#
#           ddb.from_parquet("table.parquet").df()
#           ddb.from_parquet("table.parquet").arrow().to_pandas()

from ._json import *  # noqa
from ._ndarray import *  # noqa
from ._pickle import *  # noqa
from .base import *  # noqa
from .inference import *  # noqa
