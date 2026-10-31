from enum import StrEnum

from public import public

from baikal.binance_converter._data_granularity import DataGranularity


@public
class Interval(StrEnum):
    ONE_SECOND = "1s"
    ONE_MINUTE = "1m"
    THREE_MINUTES = "3m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    TWO_HOURS = "2h"
    FOUR_HOURS = "4h"
    SIX_HOURS = "6h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    ONE_DAY = "1d"
    THREE_DAYS = "3d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1mo"


@public
class DataType(StrEnum):
    AGGREGATED_TRADES = "aggTrades"
    KLINES = "klines"
    TRADES = "trades"

    # USD-M & COIN-M futures only
    INDEX_PRICE_KLINES = "indexPriceKlines"
    MARK_PRICE_KLINES = "markPriceKlines"
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"
    METRICS = "metrics"
    FUNDING_RATE = "fundingRate"

    @property
    def granularities(self) -> tuple[DataGranularity, ...]:
        """Granularities the archive publishes the data type files with."""

        match self:
            case DataType.METRICS:
                return (DataGranularity.DAILY,)
            case DataType.FUNDING_RATE:
                return (DataGranularity.MONTHLY,)

        return DataGranularity.MONTHLY, DataGranularity.DAILY

    @property
    def is_klines(self) -> bool:
        """Files share the KLines format, see `load_klines`."""

        return self in {
            DataType.KLINES,
            DataType.INDEX_PRICE_KLINES,
            DataType.MARK_PRICE_KLINES,
            DataType.PREMIUM_INDEX_KLINES,
        }


# Values are directories of the archive.
@public
class InstrumentType(StrEnum):
    SPOT = "spot"
    USD_M_FUTURES = "futures/um"
    COIN_M_FUTURES = "futures/cm"
    OPTION = "option"
