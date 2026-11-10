from datetime import date
from email.message import Message
from http.client import IncompleteRead
from pathlib import PurePosixPath
from urllib.error import HTTPError, URLError

import pytest

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.archive import archive_file
from baikal.binance_converter.config import Config
from baikal.binance_converter.download import listing
from baikal.binance_converter.download.download import archive_url
from baikal.binance_converter.download.fetch import is_transient
from baikal.binance_converter.download.listing import (
    ArchiveFile,
    parse_file_date,
    parse_listing,
)
from baikal.binance_converter.enums import DataType, InstrumentType, Interval

HEADER = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<Name>data.binance.vision</Name>"
)


def contents(key: str, size: int) -> str:
    return f"<Contents><Key>{key}</Key><Size>{size}</Size></Contents>"


def test_delimited_listing_continues_from_next_marker() -> None:
    prefix = "data/futures/um/monthly/klines/"
    content = (
        f"{HEADER}<Prefix>{prefix}</Prefix>"
        f"<NextMarker>{prefix}ETHUSDT/</NextMarker><IsTruncated>true</IsTruncated>"
        f"<CommonPrefixes><Prefix>{prefix}BTCUSDT/</Prefix></CommonPrefixes>"
        f"<CommonPrefixes><Prefix>{prefix}ETHUSDT/</Prefix></CommonPrefixes>"
        "</ListBucketResult>"
    )

    page = parse_listing(content.encode())

    assert page.prefixes == [f"{prefix}BTCUSDT/", f"{prefix}ETHUSDT/"]
    assert page.files == []
    assert page.next_marker == f"{prefix}ETHUSDT/"


def test_listing_skips_checksums_and_continues_from_last_key() -> None:
    folder = "data/futures/um/monthly/klines/BTCUSDT/1d/"
    content = (
        f"{HEADER}<IsTruncated>true</IsTruncated>"
        + contents(f"{folder}BTCUSDT-1d-2020-01.zip", 1000)
        + contents(f"{folder}BTCUSDT-1d-2020-01.zip.CHECKSUM", 100)
        + contents(f"{folder}BTCUSDT-1d-2020-02.zip", 900)
        + "</ListBucketResult>"
    )

    page = parse_listing(content.encode())

    assert page.files == [
        ArchiveFile(
            path=PurePosixPath(
                "futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2020-01.zip"
            ),
            size=1000,
        ),
        ArchiveFile(
            path=PurePosixPath(
                "futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2020-02.zip"
            ),
            size=900,
        ),
    ]
    assert page.next_marker == f"{folder}BTCUSDT-1d-2020-02.zip"


def test_last_listing_page() -> None:
    content = f"{HEADER}<IsTruncated>false</IsTruncated></ListBucketResult>"

    assert parse_listing(content.encode()).next_marker is None


def test_listed_paths_match_archive_files() -> None:
    config = Config(
        data_type=DataType.METRICS,
        instrument_type=InstrumentType.USD_M_FUTURES,
        instrument="BTCUSDT",
    )
    path = archive_file(config, DataGranularity.DAILY, date(2024, 3, 4))
    content = (
        f"{HEADER}<IsTruncated>false</IsTruncated>"
        + contents(f"data/{path}", 10)
        + "</ListBucketResult>"
    )

    (file,) = parse_listing(content.encode()).files

    assert file.path == path
    assert file.file_date(DataGranularity.DAILY) == date(2024, 3, 4)


@pytest.mark.parametrize(
    ("name", "granularity", "expected"),
    [
        ("BTCUSDT-1m-2020-01.zip", DataGranularity.MONTHLY, date(2020, 1, 1)),
        ("BTCUSDT-metrics-2020-09-30.zip", DataGranularity.DAILY, date(2020, 9, 30)),
        (
            "1000PEPEUSDT-fundingRate-2024-05.zip",
            DataGranularity.MONTHLY,
            date(2024, 5, 1),
        ),
    ],
)
def test_parse_file_date(
    name: str, granularity: DataGranularity, expected: date
) -> None:
    assert parse_file_date(name, granularity) == expected


def test_parse_file_date_of_other_granularity() -> None:
    with pytest.raises(ValueError, match="no daily date"):
        parse_file_date("BTCUSDT-1m-2020-01.zip", DataGranularity.DAILY)


def test_url_of_non_ascii_instrument_is_percent_encoded() -> None:
    config = Config(
        data_type=DataType.KLINES,
        instrument_type=InstrumentType.USD_M_FUTURES,
        instrument="币安人生USDT",
        interval=Interval.ONE_DAY,
    )

    file = archive_file(config, DataGranularity.MONTHLY, date(2025, 10, 1))

    assert file == PurePosixPath(
        "futures/um/monthly/klines/币安人生USDT/1d/币安人生USDT-1d-2025-10.zip"
    )
    assert archive_url(file, base_url="https://x") == (
        "https://x/futures/um/monthly/klines/%E5%B8%81%E5%AE%89%E4%BA%BA%E7%94%9FUSDT"
        "/1d/%E5%B8%81%E5%AE%89%E4%BA%BA%E7%94%9FUSDT-1d-2025-10.zip"
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (HTTPError("https://x", 404, "Not Found", Message(), None), False),
        (HTTPError("https://x", 403, "Forbidden", Message(), None), False),
        (HTTPError("https://x", 429, "Too Many Requests", Message(), None), True),
        (HTTPError("https://x", 503, "Service Unavailable", Message(), None), True),
        (URLError("timed out"), True),
        (TimeoutError(), True),
        (ConnectionResetError(), True),
        (IncompleteRead(b"partial", 100), True),
    ],
)
def test_transient_errors(error: OSError | IncompleteRead, *, expected: bool) -> None:
    assert is_transient(error) is expected


def test_files_without_a_date_are_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    folder = "data/futures/um/daily/metrics/MATICUSDT/"
    page = parse_listing(
        (
            f"{HEADER}<IsTruncated>false</IsTruncated>"
            + contents(f"{folder}MATICUSDT-metrics-2022-01-02.zip", 10)
            + contents(f"{folder}MATICUSDT-metrics-1993.zip", 10)
            + contents(f"{folder}MATICUSDT-metrics-2022-01-01.zip", 10)
            + "</ListBucketResult>"
        ).encode()
    )
    monkeypatch.setattr(listing, "list_pages", lambda *_args, **_kwargs: iter([page]))
    config = Config(
        data_type=DataType.METRICS,
        instrument_type=InstrumentType.USD_M_FUTURES,
        instrument="MATICUSDT",
    )

    files = listing.list_files(config, DataGranularity.DAILY)

    assert [file.path.name for file in files] == [
        "MATICUSDT-metrics-2022-01-01.zip",
        "MATICUSDT-metrics-2022-01-02.zip",
    ]
