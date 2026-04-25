from datetime import datetime
from pathlib import Path

from click import Choice, DateTime, Path as ClickPath, argument, command, option

from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import DataType, InstrumentType, Interval
from baikal.binance_converter.klines.klines import load_klines


@command("klines")
@argument("root", required=True, type=ClickPath(exists=True, file_okay=False))
@argument("destination", required=True, type=ClickPath(dir_okay=False))
@option("--instrument-type", required=True, type=Choice(InstrumentType))
@option("--interval", required=True, type=Choice(Interval))
@option("--instrument", required=True, type=str)
@option(
    "--start", required=True, type=DateTime(), help="Inverval start, inclusive (UTC)"
)
@option("--end", required=True, type=DateTime(), help="Interval end, exclusive (UTC)")
def save_klines(
    root: str,
    destination: str,
    instrument_type: InstrumentType,
    interval: Interval,
    instrument: str,
    start: datetime,
    end: datetime,
) -> None:
    config = Config(
        data_type=DataType.KLINES,
        instrument_type=instrument_type,
        interval=interval,
        instrument=instrument,
    )

    klines = load_klines(Path(root), config, start, end)
    klines.collect().write_parquet(destination, mkdir=True)
