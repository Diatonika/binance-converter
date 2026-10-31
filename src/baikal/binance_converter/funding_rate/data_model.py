from typing import Annotated

from pandera.api.polars.model_config import BaseConfig
from pandera.polars import DataFrameModel, Field
from pandera.typing.polars import Series
from polars import Datetime, Float64, Int64
from public import public


@public
class FundingRateDataModel(DataFrameModel):
    """USD-M perpetual futures funding rate, known at its settlement time.

    ``date_time`` is the archive ``calc_time``: the settlement time of the rate,
    which longs pay shorts (negative rates: shorts pay longs) for the preceding
    ``funding_interval_hours``.
    """

    date_time: Series[Annotated[Datetime, "us", "UTC"]] = Field(unique=True)
    funding_interval_hours: Int64
    funding_rate: Float64

    class Config(BaseConfig):
        coerce = True
