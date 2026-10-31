from datetime import datetime, timedelta
from pathlib import Path

from pandera.typing.polars import LazyFrame
from polars import (
    Datetime,
    Float64,
    LazyFrame as PolarLazyFrame,
    String,
    col,
    concat,
    lit,
    scan_csv,
    when,
)
from public import public
from rich.progress import Progress

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter._schema import polars_schema
from baikal.binance_converter.archive import (
    as_utc,
    file_dates,
    local_file,
    read_zip_csv,
)
from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import DataType
from baikal.binance_converter.metrics.data_model import (
    METRICS_PERIOD,
    PERIOD_START_STAMP_SINCE,
    MetricsDataModel,
)

RAW_SCHEMA = {
    "create_time": String,
    "symbol": String,
    "sum_open_interest": Float64,
    "sum_open_interest_value": Float64,
    "count_toptrader_long_short_ratio": Float64,
    "sum_toptrader_long_short_ratio": Float64,
    "count_long_short_ratio": Float64,
    "sum_taker_long_short_vol_ratio": Float64,
}

# Close times end a millisecond before the next period, as KLines close times do.
MILLISECOND = timedelta(milliseconds=1)


@public
def load_metrics(
    root: Path, config: Config, start: datetime, end: datetime
) -> LazyFrame[MetricsDataModel]:
    """Loads local **binance.vision** futures metrics on [`start`, `end`).

    The archive publishes daily files only. Duplicated rows of a period are dropped.
    Values may be null and periods may be missing.
    """

    if config.data_type != DataType.METRICS:
        message = f"Metrics data type is required, but got {config}."
        raise ValueError(message)

    chunks: list[PolarLazyFrame] = []
    with Progress(transient=True) as progress:
        task = progress.add_task(f"{config.instrument} metrics")

        for file_date in file_dates(DataGranularity.DAILY, start, end):
            path = local_file(root, config, DataGranularity.DAILY, file_date)
            if path is not None:
                chunks.append(load_metrics_zip(path))

            progress.advance(task)

        progress.update(task, completed=100, refresh=True)

    columns = list(MetricsDataModel.to_schema().columns)
    if not chunks:
        return MetricsDataModel.validate(
            PolarLazyFrame(schema=polars_schema(MetricsDataModel)), lazy=True
        )

    utc_start, utc_end = as_utc(start), as_utc(end)
    data = (
        concat(chunks, how="vertical")
        .filter(col("date_time") >= utc_start, col("date_time") < utc_end)
        .unique("date_time", keep="last", maintain_order=True)
        .sort("date_time")
        .select(columns)
    )

    return MetricsDataModel.validate(data, lazy=True)


@public
def load_metrics_zip(path: Path) -> PolarLazyFrame:
    content, has_header = read_zip_csv(path)

    return (
        scan_csv(
            content,
            has_header=False,
            skip_rows=int(has_header),
            schema=RAW_SCHEMA,
        )
        .with_columns(
            col("create_time").str.to_datetime(
                "%Y-%m-%d %H:%M:%S", time_unit="us", time_zone="UTC"
            )
        )
        .with_columns(
            when(col("create_time") < PERIOD_START_STAMP_SINCE)
            .then(col("create_time") - METRICS_PERIOD)
            .otherwise(col("create_time"))
            .alias("date_time")
        )
        .with_columns(
            (col("date_time") + lit(METRICS_PERIOD) - lit(MILLISECOND))
            .cast(Datetime("us", "UTC"))
            .alias("close_date_time")
        )
    )
