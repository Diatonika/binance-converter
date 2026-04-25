from attrs import frozen
from public import public

from baikal.binance_converter.enums import (
    DataType,
    InstrumentType,
    Interval,
)


@public
@frozen
class Config:
    data_type: DataType
    instrument_type: InstrumentType
    interval: Interval
    instrument: str
