from datetime import UTC, datetime, timedelta
from typing import Annotated

from pandera.api.polars.model_config import BaseConfig
from pandera.polars import DataFrameModel, Field
from pandera.typing.polars import Series
from polars import Datetime, Float64
from public import public

# Metrics describe a 5-minute period: the taker volume ratio matches KLines of the
# period, the open interest is a snapshot at its end. Files before 2024-03-04 stamp
# the period with its end (`create_time`), files since then with its start.
METRICS_PERIOD = timedelta(minutes=5)
PERIOD_START_STAMP_SINCE = datetime(2024, 3, 4, tzinfo=UTC)


@public
class MetricsDataModel(DataFrameModel):
    """USD-M futures trading metrics, every 5 minutes.

    ``date_time`` is the period start, whichever way the archive ``create_time``
    stamps it. Values are known at the period end only (``close_date_time``).
    """

    date_time: Series[Annotated[Datetime, "us", "UTC"]] = Field(unique=True)
    close_date_time: Series[Annotated[Datetime, "us", "UTC"]]

    # Open interest, in base asset & in quote asset
    sum_open_interest: Float64 = Field(nullable=True)
    sum_open_interest_value: Float64 = Field(nullable=True)

    # Long / short ratios of top trader accounts & positions, of all accounts
    count_toptrader_long_short_ratio: Float64 = Field(nullable=True)
    sum_toptrader_long_short_ratio: Float64 = Field(nullable=True)
    count_long_short_ratio: Float64 = Field(nullable=True)

    # Taker buy / sell volume ratio
    sum_taker_long_short_vol_ratio: Float64 = Field(nullable=True)

    class Config(BaseConfig):
        coerce = True
