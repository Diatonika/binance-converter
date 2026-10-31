from datetime import datetime
from pathlib import Path

from pandera.typing.polars import LazyFrame
from polars import (
    Float64,
    Int64,
    LazyFrame as PolarLazyFrame,
    col,
    concat,
    from_epoch,
    scan_csv,
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
from baikal.binance_converter.funding_rate.data_model import FundingRateDataModel

RAW_SCHEMA = {
    "calc_time": Int64,
    "funding_interval_hours": Int64,
    "last_funding_rate": Float64,
}


@public
def load_funding_rate(
    root: Path, config: Config, start: datetime, end: datetime
) -> LazyFrame[FundingRateDataModel]:
    """Loads local **binance.vision** futures funding rates settled on [`start`, `end`).

    The archive publishes monthly files only.
    """

    if config.data_type != DataType.FUNDING_RATE:
        message = f"Funding rate data type is required, but got {config}."
        raise ValueError(message)

    chunks: list[PolarLazyFrame] = []
    with Progress(transient=True) as progress:
        task = progress.add_task(f"{config.instrument} funding rate")

        for file_date in file_dates(DataGranularity.MONTHLY, start, end):
            path = local_file(root, config, DataGranularity.MONTHLY, file_date)
            if path is not None:
                chunks.append(load_funding_rate_zip(path))

            progress.advance(task)

        progress.update(task, completed=100, refresh=True)

    columns = list(FundingRateDataModel.to_schema().columns)
    if not chunks:
        return FundingRateDataModel.validate(
            PolarLazyFrame(schema=polars_schema(FundingRateDataModel)), lazy=True
        )

    data = (
        concat(chunks, how="vertical")
        .filter(col("date_time") >= as_utc(start), col("date_time") < as_utc(end))
        .unique("date_time", keep="last", maintain_order=True)
        .sort("date_time")
        .select(columns)
    )

    return FundingRateDataModel.validate(data, lazy=True)


@public
def load_funding_rate_zip(path: Path) -> PolarLazyFrame:
    content, has_header = read_zip_csv(path)

    return scan_csv(
        content,
        has_header=False,
        skip_rows=int(has_header),
        schema=RAW_SCHEMA,
    ).select(
        from_epoch("calc_time", "ms").dt.replace_time_zone("UTC").alias("date_time"),
        col("funding_interval_hours"),
        col("last_funding_rate").alias("funding_rate"),
    )
