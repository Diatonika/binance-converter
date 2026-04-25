from typing import Annotated, Any

from pandera.api.polars.model_config import BaseConfig
from pandera.polars import DataFrameModel
from pandera.typing.polars import Series
from polars import Datetime, Float64, Int64, Schema
from public import public


@public
class DataModel(DataFrameModel):
    date_time: Series[Annotated[Datetime, "us", "UTC"]]

    open: Float64
    high: Float64
    low: Float64
    close: Float64
    volume: Float64

    close_date_time: Series[Annotated[Datetime, "us", "UTC"]]

    quote_volume: Float64
    trades_count: Int64
    taker_buy_base_volume: Float64
    taker_buy_quote_volume: Float64

    @classmethod
    def polar_schema(cls) -> Schema:
        types: dict[str, Any] = {
            name: dtype.type for name, dtype in cls.to_schema().dtypes.items()
        }

        return Schema(types, check_dtypes=True)

    class Config(BaseConfig):
        coerce = True
