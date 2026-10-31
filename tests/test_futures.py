from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from click.testing import CliRunner
from polars import read_parquet

from baikal.binance_converter.config import Config
from baikal.binance_converter.enums import DataType, InstrumentType, Interval
from baikal.binance_converter.funding_rate.command import save_funding_rate
from baikal.binance_converter.funding_rate.funding_rate import load_funding_rate
from baikal.binance_converter.klines.klines import load_klines
from baikal.binance_converter.metrics.command import save_metrics
from baikal.binance_converter.metrics.metrics import load_metrics

METRICS = Config(
    data_type=DataType.METRICS,
    instrument_type=InstrumentType.USD_M_FUTURES,
    instrument="BTCUSDT",
)

FUNDING_RATE = Config(
    data_type=DataType.FUNDING_RATE,
    instrument_type=InstrumentType.USD_M_FUTURES,
    instrument="BTCUSDT",
)


def test_premium_index_klines_with_header(datadir: Path) -> None:
    klines = load_klines(
        datadir,
        Config(
            data_type=DataType.PREMIUM_INDEX_KLINES,
            instrument_type=InstrumentType.USD_M_FUTURES,
            instrument="BTCUSDT",
            interval=Interval.ONE_MINUTE,
        ),
        datetime(2025, 5, 1, tzinfo=UTC),
        datetime(2025, 5, 3, tzinfo=UTC),
    ).collect()

    assert klines.height == 2 * 24 * 60
    assert klines.item(0, "date_time") == datetime(2025, 5, 1, tzinfo=UTC)
    assert klines.item(0, "open") == pytest.approx(-0.00052824)


def test_klines_require_interval(datadir: Path) -> None:
    with pytest.raises(ValueError, match="interval"):
        load_klines(
            datadir,
            METRICS,
            datetime(2025, 5, 1, tzinfo=UTC),
            datetime(2025, 5, 2, tzinfo=UTC),
        )


def test_metrics_deduplicated(datadir: Path) -> None:
    # 2020-09 files repeat every row.
    metrics = load_metrics(
        datadir,
        METRICS,
        datetime(2020, 8, 31, tzinfo=UTC),
        datetime(2020, 9, 3, tzinfo=UTC),
    ).collect()

    assert metrics["date_time"].is_unique().all()
    assert metrics["date_time"].is_sorted()


def test_metrics_stamped_by_period_end_before_2024_03_04(datadir: Path) -> None:
    metrics = load_metrics(
        datadir,
        METRICS,
        datetime(2020, 8, 31, tzinfo=UTC),
        datetime(2020, 9, 3, tzinfo=UTC),
    ).collect()

    # `create_time` 2020-09-01 00:00 ends the period.
    first = metrics.row(0, named=True)
    assert first["date_time"] == datetime(2020, 8, 31, 23, 55, tzinfo=UTC)
    assert first["sum_open_interest"] == pytest.approx(39080.231)


def test_metrics_close_date_time(datadir: Path) -> None:
    metrics = load_metrics(
        datadir,
        METRICS,
        datetime(2025, 5, 1, tzinfo=UTC),
        datetime(2025, 5, 2, tzinfo=UTC),
    ).collect()

    assert metrics.height == 24 * 12
    assert metrics.item(0, "date_time") == datetime(2025, 5, 1, tzinfo=UTC)
    assert metrics.item(0, "close_date_time") == datetime(
        2025, 5, 1, 0, 5, tzinfo=UTC
    ) - timedelta(milliseconds=1)


def test_metrics_cli(datadir: Path, tmp_path: Path) -> None:
    destination = tmp_path / "metrics.parquet"
    result = CliRunner().invoke(
        save_metrics,
        [
            "--instrument-type",
            "futures/um",
            "--instrument",
            "BTCUSDT",
            "--start",
            "2025-05-01",
            "--end",
            "2025-05-02",
            str(datadir),
            str(destination),
        ],
    )

    assert result.exit_code == 0, result.output
    assert read_parquet(destination).height == 24 * 12


def test_funding_rate(datadir: Path) -> None:
    funding_rate = load_funding_rate(
        datadir,
        FUNDING_RATE,
        datetime(2025, 4, 15, tzinfo=UTC),
        datetime(2025, 5, 15, tzinfo=UTC),
    ).collect()

    # Every 8 hours, files of both months contribute.
    assert funding_rate.height == 30 * 3
    assert funding_rate.item(0, "date_time") == datetime(2025, 4, 15, tzinfo=UTC)
    assert (funding_rate["funding_interval_hours"] == 8).all()


def test_funding_rate_cli(datadir: Path, tmp_path: Path) -> None:
    destination = tmp_path / "funding_rate.parquet"
    result = CliRunner().invoke(
        save_funding_rate,
        [
            "--instrument-type",
            "futures/um",
            "--instrument",
            "BTCUSDT",
            "--start",
            "2020-01-01",
            "--end",
            "2020-02-01",
            str(datadir),
            str(destination),
        ],
    )

    assert result.exit_code == 0, result.output

    funding_rate = read_parquet(destination)
    assert funding_rate.height == 31 * 3
    assert funding_rate.item(0, "funding_rate") == pytest.approx(-0.00012359)
