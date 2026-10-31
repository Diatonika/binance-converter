from datetime import datetime
from pathlib import Path

from click import Choice, Path as ClickPath, argument, command, option

from baikal.binance_converter.cli import (
    end_option,
    instrument_option,
    instrument_type_option,
    start_option,
)
from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import DataType, InstrumentType, Interval
from baikal.binance_converter.klines.klines import load_klines


@command("klines")
@argument("root", required=True, type=ClickPath(exists=True, file_okay=False))
@argument("destination", required=True, type=ClickPath(dir_okay=False))
@instrument_type_option
@option(
    "--data-type",
    type=Choice(member.value for member in DataType if member.is_klines),
    default=DataType.KLINES.value,
    show_default=True,
    help="KLines-like data type, others than `klines` are futures only.",
)
@option("--interval", required=True, type=Choice(member.value for member in Interval))
@instrument_option
@start_option
@end_option
def save_klines(
    root: str,
    destination: str,
    instrument_type: str,
    data_type: str,
    interval: str,
    instrument: str,
    start: datetime,
    end: datetime,
) -> None:
    """Convert KLines archive files within ROOT into DESTINATION parquet file."""

    config = Config(
        data_type=DataType(data_type),
        instrument_type=InstrumentType(instrument_type),
        instrument=instrument,
        interval=Interval(interval),
    )

    klines = load_klines(Path(root), config, start, end)
    klines.collect().write_parquet(destination, mkdir=True)
