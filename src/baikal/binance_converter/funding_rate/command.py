from datetime import datetime
from pathlib import Path

from click import Path as ClickPath, argument, command

from baikal.binance_converter.cli import (
    end_option,
    instrument_option,
    instrument_type_option,
    start_option,
)
from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import DataType, InstrumentType
from baikal.binance_converter.funding_rate.funding_rate import load_funding_rate


@command("funding-rate")
@argument("root", required=True, type=ClickPath(exists=True, file_okay=False))
@argument("destination", required=True, type=ClickPath(dir_okay=False))
@instrument_type_option
@instrument_option
@start_option
@end_option
def save_funding_rate(
    root: str,
    destination: str,
    instrument_type: str,
    instrument: str,
    start: datetime,
    end: datetime,
) -> None:
    """Convert futures funding rate archive files within ROOT into DESTINATION parquet
    file.
    """

    config = Config(
        data_type=DataType.FUNDING_RATE,
        instrument_type=InstrumentType(instrument_type),
        instrument=instrument,
    )

    funding_rate = load_funding_rate(Path(root), config, start, end)
    funding_rate.collect().write_parquet(destination, mkdir=True)
