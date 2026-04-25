from datetime import UTC, datetime
from pathlib import Path

from click.testing import CliRunner
from polars import len as length, read_parquet

from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import DataType, InstrumentType, Interval
from baikal.binance_converter.klines.command import save_klines
from baikal.binance_converter.klines.klines import load_klines


def test_aggregation(datadir: Path) -> None:
    lazy = load_klines(
        datadir,
        Config(
            DataType.KLINES,
            InstrumentType.SPOT,
            Interval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime(2020, 1, 30, tzinfo=UTC),
        datetime(2020, 3, 2, tzinfo=UTC),
        ambiguity_column="ambiguity",
    )

    klines = lazy.collect()

    assert klines.null_count().sum_horizontal().item() == 0
    assert klines.select(length()).item() == 44_226


def test_microsecond_conversion(datadir: Path) -> None:
    lazy = load_klines(
        datadir,
        Config(
            DataType.KLINES,
            InstrumentType.SPOT,
            Interval.ONE_MINUTE,
            "BTCUSDT",
        ),
        datetime(2024, 12, 31, tzinfo=UTC),
        datetime(2025, 1, 2, tzinfo=UTC),
        ambiguity_column="ambiguity",
    )

    klines = lazy.collect()
    assert klines.select(length()).item() == 2
    assert klines.item(0, "date_time") == datetime(2024, 12, 31, 23, 59, tzinfo=UTC)
    assert klines.item(1, "date_time") == datetime(2025, 1, 1, 0, 0, tzinfo=UTC)


def test_cli(datadir: Path, tmp_path: Path) -> None:
    runner = CliRunner()
    destination = f"{tmp_path}/result.parquet"
    result = runner.invoke(
        save_klines,
        [
            "--instrument-type",
            "SPOT",
            "--interval",
            "ONE_MINUTE",
            "--instrument",
            "BTCUSDT",
            "--start",
            "2020-1-30",
            "--end",
            "2020-03-02",
            f"{datadir}",
            destination,
        ],
    )

    assert result.exit_code == 0

    klines = read_parquet(destination)
    assert klines.null_count().sum_horizontal().item() == 0
    assert klines.select(length()).item() == 44_226
