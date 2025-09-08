import datetime
import logging

from pathlib import Path
from zipfile import ZipFile

from pandera.typing.polars import LazyFrame
from polars import (
    Boolean,
    Expr,
    Int16,
    Int64,
    LazyFrame as PolarLazyFrame,
    coalesce,
    col,
    concat,
    datetime_range,
    from_epoch,
    lit,
    scan_csv,
)
from rich.progress import Progress

from baikal.common.models import OHLCV
from baikal.common.rich import RichConsoleStack, with_handler
from baikal.common.rich.progress import TimeFraction
from baikal.converters.binance._data_granularity import BinanceDataGranularity
from baikal.converters.binance._ohlcv import BinanceOHLCV
from baikal.converters.binance.config import BinanceDataConfig
from baikal.converters.binance.enums import BinanceInstrumentType


class BinanceConverter:
    LOGGER = logging.getLogger(__name__)

    def __init__(self, root: Path) -> None:
        if not root.exists():
            error = f"Binance data directory {root} not found."
            raise OSError(error)

        if not root.is_dir():
            error = f"Invalid Binance data directory {root} (not a directory)."
            raise OSError(error)

        self._root = root

    @with_handler(LOGGER)
    def load_ohlcv(
        self,
        config: BinanceDataConfig,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        ambiguity_column: str | None = None,
    ) -> LazyFrame[OHLCV]:
        """Loads local **binance.vision** OHLCV data.

        Loads **binance.vision** OHLCV bulk data on left-closed
        interval [`start`, `end`) from local directory.

        Parameters
        ----------
        config: BinanceDataConfig
        start : datetime.datetime
        end : datetime.datetime
        ambiguity_column : str, optional
            Include "ambiguous" column in resulting frame.
            Default is None, which means no ambiguity column is produced.

        Returns
        -------
        LazyFrame[OHLCV]
            OHLCV time series data.

        Notes
        -----

        Returned time-series data cannot contain null and NaN values,
        but may contain time gaps due to missing data.

        Warnings
        --------

        In case of inconsistencies between daily and monthly data, warning is logged.
        """
        daily_data = self._load_ohlcv(config, start, end, BinanceDataGranularity.DAILY)
        monthly_data = self._load_ohlcv(
            config, start, end, BinanceDataGranularity.MONTHLY
        )

        filled_data = (
            PolarLazyFrame()
            .with_columns(
                date_time=datetime_range(start, end, config.interval, closed="left"),
                open=lit(None),
                high=lit(None),
                low=lit(None),
                close=lit(None),
                volume=lit(None),
            )
            .join(
                daily_data.select(OHLCV.column_names()),
                how="left",
                on="date_time",
                coalesce=False,
                maintain_order="left",
                suffix="_daily",
            )
            .join(
                monthly_data.select(OHLCV.column_names()),
                how="left",
                on="date_time",
                coalesce=False,
                maintain_order="left",
                suffix="_monthly",
            )
            .with_columns(
                open=coalesce("open_daily", "open_monthly"),
                high=coalesce("high_daily", "high_monthly"),
                low=coalesce("low_daily", "low_monthly"),
                close=coalesce("close_daily", "close_monthly"),
                volume=coalesce("volume_daily", "volume_monthly"),
            )
            .filter(col(name).is_not_null() for name in OHLCV.column_names())
        )

        schema = OHLCV.polar_schema()
        if ambiguity_column is not None:
            generator = (
                col(f"{name}_daily").ne_missing(col(f"{name}_monthly"))
                for name in OHLCV.column_names()
            )

            schema[ambiguity_column] = Boolean()
            filled_data = filled_data.with_columns(
                lit(value=True).or_(*generator).alias(ambiguity_column)
            )

        return OHLCV.validate(filled_data.select(schema.keys()), lazy=True)

    def _load_ohlcv(
        self,
        config: BinanceDataConfig,
        start: datetime.datetime,
        end: datetime.datetime,
        granularity: BinanceDataGranularity,
    ) -> LazyFrame[BinanceOHLCV]:
        chunks: list[PolarLazyFrame] = []
        time_tracker = TimeFraction(start, end)

        with Progress(console=RichConsoleStack.active(), transient=True) as progress:
            task = progress.add_task(
                f"{granularity} {config.symbol}-{config.interval} OHLCV"
            )

            chunk_date_time = start
            while chunk_date_time < end:
                chunk = self._load_ohlcv_file(
                    config, chunk_date_time.date(), granularity
                )

                if chunk is not None:
                    chunks.append(chunk)

                chunk_date_time = granularity.next_chunk(chunk_date_time)

                completed_percentage = time_tracker.fraction(chunk_date_time) * 100
                progress.update(task, completed=completed_percentage)

            progress.update(task, completed=100)

        if not len(chunks):
            return LazyFrame[BinanceOHLCV]({}, BinanceOHLCV.polar_schema())

        data = concat(chunks, how="vertical", rechunk=False)
        return BinanceOHLCV.validate(data, lazy=True)

    def _load_ohlcv_file(
        self,
        config: BinanceDataConfig,
        date: datetime.date,
        granularity: BinanceDataGranularity,
    ) -> LazyFrame[BinanceOHLCV] | None:
        path = self._find_file_path(config, date, granularity)
        if path is None:
            return None

        raw_schema = BinanceOHLCV.polar_schema() | {
            "date_time": Int64,
            "close_date_time": Int64,
            "ignore": Int16,
        }

        raw_data = scan_csv(
            ZipFile(path).read(path.with_suffix(".csv").name),
            has_header=False,
            new_columns=list(BinanceOHLCV.column_names()),
            schema=raw_schema,
        ).with_columns(
            date_time=self._from_unix(config, "date_time", date),
            close_date_time=self._from_unix(config, "close_date_time", date),
        )

        return BinanceOHLCV.validate(
            raw_data.select(BinanceOHLCV.column_names()), lazy=True
        )

    def _find_file_path(
        self,
        config: BinanceDataConfig,
        date: datetime.date,
        granularity: BinanceDataGranularity,
    ) -> Path | None:
        file_name = (
            f"{config.symbol}-{config.interval}-{granularity.file_date(date)}.zip"
        )

        folder = (
            Path(config.instrument_type)
            / granularity
            / config.data_type
            / config.symbol
            / config.interval
        )

        path = self._root / folder / file_name
        return path if path.exists() and path.is_file() else None

    @staticmethod
    def _from_unix(
        config: BinanceDataConfig, unix_column: str, date: datetime.date
    ) -> Expr:
        if (
            config.instrument_type == BinanceInstrumentType.SPOT
            and date >= datetime.date(2025, 1, 1)
        ):
            return from_epoch(unix_column, "us")

        return from_epoch(unix_column, "ms")
