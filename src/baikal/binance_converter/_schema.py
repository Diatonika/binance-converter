from typing import Any

from pandera.polars import DataFrameModel
from polars import Schema
from public import public


@public
def polars_schema(model: type[DataFrameModel]) -> Schema:
    """Polars schema of ``model`` columns, e.g. for an empty frame."""

    types: dict[str, Any] = {
        name: dtype.type for name, dtype in model.to_schema().dtypes.items()
    }

    return Schema(types, check_dtypes=True)
