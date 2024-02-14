"""Monkeypatch PandasArray class.

Pandas 2.x renamed and moved the PandasArray class.
"""

try:
    # Pandas >= 2.x
    from pandas.arrays import NumpyExtensionArray as PandasArray
except ImportError:
    # Pandas < 2.0
    from pandas.core.arrays import PandasArray

__all__ = ["PandasArray"]
