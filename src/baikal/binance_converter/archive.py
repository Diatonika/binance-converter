from collections.abc import Iterator
from datetime import UTC, date, datetime, time
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

from dateutil.relativedelta import relativedelta
from public import public

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.config import Config


@public
def archive_file(
    config: Config, granularity: DataGranularity, file_date: date
) -> PurePosixPath:
    """Path of the **binance.vision** file relative to the archive root, e.g.
    ``spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2020-01.zip`` or
    ``futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2020-09-01.zip``.
    """

    folder = (
        PurePosixPath(config.instrument_type)
        / granularity
        / config.data_type
        / config.instrument
    )

    # KLines files are named after their interval, other files after their data type.
    suffix: str = config.data_type
    if config.interval is not None:
        folder /= config.interval
        suffix = config.interval

    return (
        folder / f"{config.instrument}-{suffix}-{granularity.file_date(file_date)}.zip"
    )


@public
def file_dates(
    granularity: DataGranularity, start: datetime, end: datetime
) -> Iterator[date]:
    """Dates of the ``granularity`` files covering [`start`, `end`)."""

    match granularity:
        case DataGranularity.DAILY:
            current = start.date()
            step = relativedelta(days=1)
        case DataGranularity.MONTHLY:
            current = start.date().replace(day=1)
            step = relativedelta(months=1)

    while datetime.combine(current, time(), tzinfo=end.tzinfo) < end:
        yield current
        current += step


@public
def local_file(
    root: Path, config: Config, granularity: DataGranularity, file_date: date
) -> Path | None:
    """Local copy of the archive file within ``root`` directory, if present."""

    path = root / archive_file(config, granularity, file_date)
    return path if path.is_file() else None


@public
def read_zip_csv(path: Path) -> tuple[bytes, bool]:
    """CSV content of the archive ``path`` file & whether it has a header row.

    Older files have no header, newer ones (and re-published older ones) have it.
    """

    with ZipFile(path) as archive:
        content = archive.read(path.with_suffix(".csv").name)

    return content, not content[:1].isdigit()


@public
def as_utc(date_time: datetime) -> datetime:
    """Naive date times are UTC, as command line options are."""

    return date_time if date_time.tzinfo else date_time.replace(tzinfo=UTC)
