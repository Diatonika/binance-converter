from datetime import UTC, date, datetime
from pathlib import Path
from typing import IO
from zipfile import ZipFile

from pandera.typing.polars import LazyFrame
from polars import (
    Expr,
    Float64,
    Int16,
    Int64,
    LazyFrame as PolarLazyFrame,
    any_horizontal,
    coalesce,
    col,
    concat,
    datetime_range,
    from_epoch,
    lit,
    scan_csv,
    when,
)
from public import private, public
from rich.progress import Progress

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import InstrumentType
from baikal.binance_converter.klines.data_model import DataModel

MILLISECOND_CUTOFF = int(datetime(2025, 1, 1, tzinfo=UTC).timestamp()) * 1000


@public
def load_klines(
    root: Path,
    config: Config,
    start: datetime,
    end: datetime,
    *,
    ambiguity_column: str | None = None,
) -> LazyFrame[DataModel]:
    """Loads local **binance.vision** KLines data.

    Loads **binance.vision** KLines bulk data on left-closed
    interval [`start`, `end`) from local directory.

    Parameters
    ----------
    root: Path
        Path to directory structure root.
    config: BinanceDataConfig
    start : datetime.datetime
    end : datetime.datetime
    ambiguity_column : str, optional
        Include "ambiguous" indicator column in resulting frame.
        Default is None, which means no ambiguity column is produced.

    Returns
    -------
    LazyFrame[DataModel]
        KLines time series data.

    Notes
    -----
    Returned time-series data cannot contain null and NaN values,
    but may contain time gaps due to missing data.
    """

    daily_data = load_klines_with_granularity(
        root, config, start, end, DataGranularity.DAILY
    )

    monthly_data = load_klines_with_granularity(
        root, config, start, end, DataGranularity.MONTHLY
    )

    feature_columns = tuple(
        column
        for column in DataModel.to_schema().columns
        if column != DataModel.date_time
    )

    filled_data = (
        PolarLazyFrame()
        .with_columns(
            date_time=datetime_range(
                start,
                end,
                config.interval,
                closed="left",
                time_unit="us",
                time_zone="UTC",
            ),
            **{column: lit(None) for column in feature_columns},
        )
        .join(
            daily_data,
            how="left",
            on="date_time",
            coalesce=False,
            maintain_order="left",
            suffix="_daily",
        )
        .join(
            monthly_data,
            how="left",
            on="date_time",
            coalesce=False,
            maintain_order="left",
            suffix="_monthly",
        )
        .with_columns(
            **{
                column: coalesce(f"{column}_daily", f"{column}_monthly")
                for column in feature_columns
            }
        )
        .filter(col(name).is_not_null() for name in feature_columns)
    )

    column_names = list(DataModel.to_schema().columns)
    if ambiguity_column is not None:
        generator = (
            col(f"{column}_daily").ne_missing(col(f"{column}_monthly"))
            for column in feature_columns
        )

        column_names.append(ambiguity_column)
        filled_data = filled_data.with_columns(
            any_horizontal(*generator).alias(ambiguity_column)
        )

    return DataModel.validate(filled_data.select(column_names), lazy=True)


@public
def load_from_zip(path: Path, instrument_type: InstrumentType) -> LazyFrame[DataModel]:
    with ZipFile(path).open(path.with_suffix(".csv").name) as opened:
        return load_from_csv(opened.read(), instrument_type)


@public
def load_from_csv(
    csv: Path | IO[bytes] | IO[str] | bytes | str, instrument_type: InstrumentType
) -> LazyFrame[DataModel]:
    raw_schema = {
        "date_time": Int64,
        "open": Float64,
        "high": Float64,
        "low": Float64,
        "close": Float64,
        "volume": Float64,
        "close_date_time": Int64,
        "quote_volume": Float64,
        "trades_count": Int64,
        "taker_buy_base_volume": Float64,
        "taker_buy_quote_volume": Float64,
        "ignore": Int16,
    }

    raw_data = scan_csv(
        csv,
        has_header=False,
        new_columns=list(DataModel.to_schema().columns),
        schema=raw_schema,
    ).with_columns(
        date_time=parse_unix(instrument_type, "date_time"),
        close_date_time=parse_unix(instrument_type, "close_date_time"),
    )

    return DataModel.validate(
        raw_data.select(DataModel.to_schema().columns.keys()), lazy=True
    )


@private
def load_klines_with_granularity(
    root: Path,
    config: Config,
    start: datetime,
    end: datetime,
    granularity: DataGranularity,
) -> LazyFrame[DataModel]:
    chunks: list[PolarLazyFrame] = []

    with Progress(transient=True) as progress:
        task = progress.add_task(
            f"{granularity} {config.instrument}-{config.interval} KLines"
        )

        chunk_date_time = start
        while chunk_date_time < end:
            path = build_zip_path(root, config, chunk_date_time.date(), granularity)
            if path is not None:
                chunk = load_from_zip(path, config.instrument_type)
                chunks.append(chunk)

            chunk_date_time = granularity.next_chunk(chunk_date_time)
            progress.advance(task)

        progress.update(task, completed=100, refresh=True)

    if not len(chunks):
        return LazyFrame[DataModel]({}, DataModel.polar_schema())

    data = concat(chunks, how="vertical", rechunk=False)
    return DataModel.validate(data, lazy=True)


@private
def build_zip_path(
    root: Path, config: Config, date: date, granularity: DataGranularity
) -> Path | None:
    file_name = (
        f"{config.instrument}-{config.interval}-{granularity.file_date(date)}.zip"
    )

    folder = (
        Path(config.instrument_type)
        / granularity
        / config.data_type
        / config.instrument
        / config.interval
    )

    path = root / folder / file_name
    return path if path.exists() and path.is_file() else None


@private
def parse_unix(instrument_type: InstrumentType, unix_column: str) -> Expr:
    if instrument_type == InstrumentType.SPOT:
        return (
            when(col(unix_column) < MILLISECOND_CUTOFF)
            .then(from_epoch(unix_column, "ms"))
            .otherwise(from_epoch(unix_column, "us"))
        )

    return from_epoch(unix_column, "ms")
