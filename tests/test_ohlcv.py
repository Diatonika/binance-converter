import datetime

from pathlib import Path

from polars import len as length

from baikal.converters.binance import (
    BinanceConverter,
    BinanceDataConfig,
    BinanceDataInterval,
    BinanceDataType,
    BinanceInstrumentType,
)


def test_monthly_ohlcv(datadir: Path) -> None:
    converter = BinanceConverter(datadir)

    lazy = converter.load_ohlcv(
        BinanceDataConfig(
            BinanceDataType.OHLCV,
            BinanceInstrumentType.SPOT,
            BinanceDataInterval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime.datetime(2020, 1, 30, tzinfo=datetime.UTC),
        datetime.datetime(2020, 3, 2, tzinfo=datetime.UTC),
        ambiguity_column="ambiguity",
    )

    ohlcv = lazy.collect()

    assert ohlcv.null_count().sum_horizontal().item() == 0
    assert ohlcv.select(length()).item() == 44_226
