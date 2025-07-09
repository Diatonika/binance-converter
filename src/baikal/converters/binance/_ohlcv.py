from typing import Annotated

from pandera.typing.polars import Series
from polars import Datetime, Float64, Int64

from baikal.common.trade.models import TradeModel


class BinanceOHLCV(TradeModel):
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
