from datetime import UTC, datetime
from pathlib import Path
from typing import IO, cast

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
from baikal.binance_converter.archive import file_dates, local_file, read_zip_csv
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
    progress: bool = False,
) -> LazyFrame[DataModel]:
    """Loads local **binance.vision** KLines data.

    Loads **binance.vision** KLines bulk data on left-closed
    interval [`start`, `end`) from local directory. KLines-like futures
    data types (premium index, index & mark price) are loaded as well.

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
    progress : bool
        Show a progress bar of the files read (e.g. from the command line).

    Returns
    -------
    LazyFrame[DataModel]
        KLines time series data.

    Notes
    -----
    Returned time-series data cannot contain null and NaN values,
    but may contain time gaps due to missing data.
    """

    if not config.data_type.is_klines or config.interval is None:
        message = f"KLines data type & interval are required, but got {config}."
        raise ValueError(message)

    daily_data = load_klines_with_granularity(
        root, config, start, end, DataGranularity.DAILY, progress=progress
    )

    monthly_data = load_klines_with_granularity(
        root, config, start, end, DataGranularity.MONTHLY, progress=progress
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
                str(config.interval),
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
    content, has_header = read_zip_csv(path)
    return load_from_csv(content, instrument_type, has_header=has_header)


@public
def load_from_csv(
    csv: Path | IO[bytes] | IO[str] | bytes | str,
    instrument_type: InstrumentType,
    *,
    has_header: bool = False,
) -> LazyFrame[DataModel]:
    return DataModel.validate(
        scan_csv_klines(csv, instrument_type, has_header=has_header), lazy=True
    )


@private
def scan_csv_klines(
    csv: Path | IO[bytes] | IO[str] | bytes | str,
    instrument_type: InstrumentType,
    *,
    has_header: bool,
) -> PolarLazyFrame:
    """KLines of the CSV, the data model columns, not validated."""

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
        skip_rows=int(has_header),
        schema=raw_schema,
    ).with_columns(
        date_time=parse_unix(instrument_type, "date_time"),
        close_date_time=parse_unix(instrument_type, "close_date_time"),
    )

    # The validation coerces to the model types: the casts do it here (UTC times)
    return raw_data.select(DataModel.to_schema().columns.keys()).cast(
        DataModel.polar_schema()
    )


@private
def load_klines_with_granularity(
    root: Path,
    config: Config,
    start: datetime,
    end: datetime,
    granularity: DataGranularity,
    *,
    progress: bool,
) -> LazyFrame[DataModel]:
    # Validated once, as the loaded frame (`load_klines`): a validation per file costs
    # more than reading it.
    chunks: list[PolarLazyFrame] = []
    with Progress(transient=True, disable=not progress) as bar:
        task = bar.add_task(
            f"{granularity} {config.instrument}-{config.interval} KLines"
        )
        for file_date in file_dates(granularity, start, end):
            path = local_file(root, config, granularity, file_date)
            if path is not None:
                chunks.append(scan_zip(path, config.instrument_type))

            bar.advance(task)

    if not chunks:
        return LazyFrame[DataModel]({}, DataModel.polar_schema())

    return cast(LazyFrame[DataModel], concat(chunks, how="vertical", rechunk=False))


@private
def scan_zip(path: Path, instrument_type: InstrumentType) -> PolarLazyFrame:
    """KLines of the archive ``path`` file, not validated."""

    content, has_header = read_zip_csv(path)
    return scan_csv_klines(content, instrument_type, has_header=has_header)


@private
def parse_unix(instrument_type: InstrumentType, unix_column: str) -> Expr:
    if instrument_type == InstrumentType.SPOT:
        return (
            when(col(unix_column) < MILLISECOND_CUTOFF)
            .then(from_epoch(unix_column, "ms"))
            .otherwise(from_epoch(unix_column, "us"))
        )

    return from_epoch(unix_column, "ms")
