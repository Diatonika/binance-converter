from attrs import define

from baikal.converters.binance.enums import (
    BinanceDataInterval,
    BinanceDataType,
    BinanceInstrumentType,
)


@define
class BinanceDataConfig:
    data_type: BinanceDataType
    instrument_type: BinanceInstrumentType
    interval: BinanceDataInterval
    symbol: str
