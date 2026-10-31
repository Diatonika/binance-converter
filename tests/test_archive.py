from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

import pytest

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.archive import archive_file, file_dates, read_zip_csv
from baikal.binance_converter.config import Config
from baikal.binance_converter.download.download import parse_checksum
from baikal.binance_converter.enums import DataType, InstrumentType, Interval


@pytest.mark.parametrize(
    ("config", "granularity", "file_date", "expected"),
    [
        (
            Config(
                data_type=DataType.KLINES,
                instrument_type=InstrumentType.SPOT,
                instrument="BTCUSDT",
                interval=Interval.ONE_MINUTE,
            ),
            DataGranularity.MONTHLY,
            date(2020, 1, 1),
            "spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2020-01.zip",
        ),
        (
            Config(
                data_type=DataType.PREMIUM_INDEX_KLINES,
                instrument_type=InstrumentType.USD_M_FUTURES,
                instrument="BTCUSDT",
                interval=Interval.FIFTEEN_MINUTES,
            ),
            DataGranularity.DAILY,
            date(2025, 5, 1),
            "futures/um/daily/premiumIndexKlines/BTCUSDT/15m/BTCUSDT-15m-2025-05-01.zip",
        ),
        (
            Config(
                data_type=DataType.METRICS,
                instrument_type=InstrumentType.USD_M_FUTURES,
                instrument="BTCUSDT",
            ),
            DataGranularity.DAILY,
            date(2020, 9, 1),
            "futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2020-09-01.zip",
        ),
        (
            Config(
                data_type=DataType.FUNDING_RATE,
                instrument_type=InstrumentType.USD_M_FUTURES,
                instrument="BTCUSDT",
            ),
            DataGranularity.MONTHLY,
            date(2020, 1, 1),
            "futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2020-01.zip",
        ),
    ],
)
def test_archive_file(
    config: Config, granularity: DataGranularity, file_date: date, expected: str
) -> None:
    assert archive_file(config, granularity, file_date) == PurePosixPath(expected)


def test_file_dates() -> None:
    start = datetime(2020, 1, 30, 12, tzinfo=UTC)
    end = datetime(2020, 3, 1, 0, 1, tzinfo=UTC)

    assert list(file_dates(DataGranularity.MONTHLY, start, end)) == [
        date(2020, 1, 1),
        date(2020, 2, 1),
        date(2020, 3, 1),
    ]

    daily = list(file_dates(DataGranularity.DAILY, start, end))
    assert daily[0] == date(2020, 1, 30)
    assert daily[-1] == date(2020, 3, 1)


def test_file_dates_end_exclusive() -> None:
    dates = file_dates(
        DataGranularity.DAILY,
        datetime(2020, 1, 1, tzinfo=UTC),
        datetime(2020, 1, 3, tzinfo=UTC),
    )

    assert list(dates) == [date(2020, 1, 1), date(2020, 1, 2)]


@pytest.mark.parametrize(
    ("content", "has_header"),
    [(b"1577836800000,1,2\n", False), (b"open_time,open,high\n1,2,3\n", True)],
)
def test_read_zip_csv(tmp_path: Path, content: bytes, *, has_header: bool) -> None:
    path = tmp_path / "BTCUSDT-1m-2020-01.zip"
    with ZipFile(path, "w") as archive:
        archive.writestr("BTCUSDT-1m-2020-01.csv", content)

    assert read_zip_csv(path) == (content, has_header)


def test_parse_checksum() -> None:
    digest = sha256(b"content").hexdigest()
    assert parse_checksum(f"{digest}  BTCUSDT-1m-2020-01.zip\n".encode()) == digest
