from datetime import datetime
from pathlib import Path

from click import Choice, IntRange, Path as ClickPath, argument, command, echo, option

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.cli import (
    end_option,
    instrument_option,
    instrument_type_option,
    start_option,
)
from baikal.binance_converter.config import Config
from baikal.binance_converter.download.download import download_archive
from baikal.binance_converter.download.listing import list_files, list_instruments
from baikal.binance_converter.enums import DataType, InstrumentType, Interval


@command("download")
@argument("root", required=True, type=ClickPath(file_okay=False, path_type=Path))
@instrument_type_option
@option("--data-type", required=True, type=Choice(member.value for member in DataType))
@option(
    "--interval",
    type=Choice(member.value for member in Interval),
    help="Interval of KLines data types, omitted for others.",
)
@instrument_option
@start_option
@end_option
@option("--workers", type=IntRange(min=1), default=8, show_default=True)
def download(
    root: Path,
    instrument_type: str,
    data_type: str,
    interval: str | None,
    instrument: str,
    start: datetime,
    end: datetime,
    workers: int,
) -> None:
    """Download **binance.vision** archive files into ROOT directory."""

    config = Config(
        data_type=DataType(data_type),
        instrument_type=InstrumentType(instrument_type),
        instrument=instrument,
        interval=Interval(interval) if interval is not None else None,
    )

    summary = download_archive(root, config, start, end, workers=workers)

    echo(
        f"{config.instrument} {config.data_type}: {summary.downloaded} downloaded, "
        f"{summary.present} present, {len(summary.missing)} missing."
    )

    for file in summary.missing:
        echo(f"Missing: {file}")


@command("list")
@instrument_type_option
@option("--data-type", required=True, type=Choice(member.value for member in DataType))
@option(
    "--granularity",
    required=True,
    type=Choice(member.value for member in DataGranularity),
)
@option("--instrument", help="List the files of the instrument, instruments otherwise.")
@option(
    "--interval",
    type=Choice(member.value for member in Interval),
    help="Interval of KLines data types, omitted for others.",
)
def list_archive(
    instrument_type: str,
    data_type: str,
    granularity: str,
    instrument: str | None,
    interval: str | None,
) -> None:
    """List **binance.vision** archive instruments of a data type, or files of an
    instrument (date, size in bytes, path)."""

    if instrument is None:
        for name in list_instruments(
            InstrumentType(instrument_type),
            DataGranularity(granularity),
            DataType(data_type),
        ):
            echo(name)

        return

    config = Config(
        data_type=DataType(data_type),
        instrument_type=InstrumentType(instrument_type),
        instrument=instrument,
        interval=Interval(interval) if interval is not None else None,
    )

    for file in list_files(config, DataGranularity(granularity)):
        echo(f"{file.file_date(DataGranularity(granularity))} {file.size} {file.path}")
