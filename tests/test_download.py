from collections.abc import Collection, Iterator
from datetime import UTC, date, datetime, timedelta
from functools import partial
from hashlib import sha256
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path, PurePosixPath
from threading import Thread
from urllib.parse import SplitResult, parse_qs, urlsplit
from zipfile import ZipFile

import pytest

from attrs import frozen

from baikal.binance_converter.config import Config
from baikal.binance_converter.download.download import (
    ArchivePeriod,
    DownloadSummary,
    download_archives,
)
from baikal.binance_converter.enums import DataType, InstrumentType, Interval

FUTURES = InstrumentType.USD_M_FUTURES
DAY = timedelta(days=1)

LISTING_HEADER = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    "<IsTruncated>false</IsTruncated>"
)


def klines_zip(name: str, days: list[date]) -> bytes:
    """A daily KLines file of the archive: a row of every day."""

    def milliseconds(day: date) -> int:
        return int(datetime.combine(day, datetime.min.time(), UTC).timestamp() * 1000)

    rows = "".join(
        f"{milliseconds(day)},1,1,1,1,10,{milliseconds(day + DAY) - 1},10,5,5,5,0\n"
        for day in days
    )
    content = BytesIO()
    with ZipFile(content, "w") as archive:
        archive.writestr(name.removesuffix(".zip") + ".csv", rows)

    return content.getvalue()


def publish(served: Path, file: str, days: list[date]) -> None:
    """An archive file & its checksum file, as data.binance.vision serves them."""

    path = served / "data" / file
    path.parent.mkdir(parents=True, exist_ok=True)
    content = klines_zip(path.name, days)
    path.write_bytes(content)
    path.with_name(f"{path.name}.CHECKSUM").write_text(
        f"{sha256(content).hexdigest()}  {path.name}\n"
    )


def days_of(start: date, end: date, *, without: tuple[int, ...] = ()) -> list[date]:
    days = [start + offset * DAY for offset in range((end - start).days)]
    return [day for day in days if day.day not in without]


class ArchiveHandler(SimpleHTTPRequestHandler):
    """Files of the served directory under /data & their S3 listing at /bucket."""

    server: "ArchiveServer"

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        self.server.requests.append(url)
        if url.path == "/bucket":
            self.send_listing(url)
        else:
            super().do_GET()

    def send_listing(self, url: SplitResult) -> None:
        query = parse_qs(url.query)
        prefix, marker = query["prefix"][0], query.get("marker", [""])[0]
        served = Path(self.directory)
        keys = sorted(
            key
            for path in served.rglob("*")
            if path.is_file()
            and (key := path.relative_to(served).as_posix()).startswith(prefix)
            and key > marker
        )
        body = (
            LISTING_HEADER
            + "".join(
                f"<Contents><Key>{key}</Key><Size>1</Size></Contents>" for key in keys
            )
            + "</ListBucketResult>"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args: object) -> None:
        pass


class ArchiveServer(ThreadingHTTPServer):
    requests: list[SplitResult]


@frozen
class Archive:
    root: Path  # Of the downloads
    server: ArchiveServer

    def download(
        self,
        periods: list[ArchivePeriod],
        known_missing: Collection[PurePosixPath] = (),
    ) -> DownloadSummary:
        address = f"http://127.0.0.1:{self.server.server_port}"
        return download_archives(
            self.root,
            periods,
            workers=4,
            base_url=f"{address}/data",
            bucket_url=f"{address}/bucket",
            known_missing=known_missing,
        )

    def listings(self) -> list[str]:
        return [
            parse_qs(url.query)["prefix"][0]
            for url in self.server.requests
            if url.path == "/bucket"
        ]

    def file_requests(self) -> list[str]:
        return [
            url.path.removeprefix("/data/")
            for url in self.server.requests
            if url.path.endswith(".zip")
        ]


@pytest.fixture
def archive(tmp_path: Path) -> Iterator[Archive]:
    """A local archive server:

    * BTC: a monthly January file, daily February files, a monthly March file without
      the 15th & the 20th, of which the 15th is in a daily file;
    * ETH: a monthly February file only, a monthly March file without the 10th, which is
      in a daily file.
    """

    served = tmp_path / "served"
    btc = "futures/um/{}/klines/BTCUSDT/1d/BTCUSDT-1d-{}.zip"
    eth = "futures/um/{}/klines/ETHUSDT/1d/ETHUSDT-1d-{}.zip"
    publish(
        served,
        btc.format("monthly", "2024-01"),
        days_of(date(2024, 1, 1), date(2024, 2, 1)),
    )
    for day in (1, 2):
        publish(served, btc.format("daily", f"2024-02-{day:02d}"), [date(2024, 2, day)])
    publish(
        served,
        btc.format("monthly", "2024-03"),
        days_of(date(2024, 3, 1), date(2024, 4, 1), without=(15, 20)),
    )
    publish(served, btc.format("daily", "2024-03-15"), [date(2024, 3, 15)])
    publish(
        served,
        eth.format("monthly", "2024-02"),
        days_of(date(2024, 2, 1), date(2024, 3, 1)),
    )
    publish(
        served,
        eth.format("monthly", "2024-03"),
        days_of(date(2024, 3, 1), date(2024, 4, 1), without=(10,)),
    )
    publish(served, eth.format("daily", "2024-03-10"), [date(2024, 3, 10)])

    server = ArchiveServer(
        ("127.0.0.1", 0), partial(ArchiveHandler, directory=str(served))
    )
    server.requests = []
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield Archive(root=tmp_path / "root", server=server)
    finally:
        server.shutdown()


def klines_periods(start: datetime, end: datetime) -> list[ArchivePeriod]:
    return [
        ArchivePeriod(
            Config(DataType.KLINES, FUTURES, symbol, Interval.ONE_DAY), start, end
        )
        for symbol in ("BTCUSDT", "ETHUSDT")
    ]


def test_many_instruments_prefer_monthly_files_then_daily_ones(
    archive: Archive,
) -> None:
    periods = klines_periods(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 3, tzinfo=UTC)
    )

    summary = archive.download(periods)

    assert summary.downloaded == 4  # BTC January & 2 February days, ETH February
    assert summary.missing == [
        # ETH has no January file, monthly or daily
        *(
            PurePosixPath(
                f"futures/um/daily/klines/ETHUSDT/1d/ETHUSDT-1d-2024-01-{day:02d}.zip"
            )
            for day in range(1, 32)
        ),
        # The monthly files the daily ones replace
        PurePosixPath("futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2024-02.zip"),
        PurePosixPath("futures/um/monthly/klines/ETHUSDT/1d/ETHUSDT-1d-2024-01.zip"),
    ]
    assert (
        archive.root / "futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-02-02.zip"
    ).is_file()
    # Folders missing many files are listed (2 months, then the days of the missing
    # ones): no requests of unpublished files
    assert sorted(archive.listings()) == [
        "data/futures/um/daily/klines/BTCUSDT/1d/",
        "data/futures/um/daily/klines/ETHUSDT/1d/",
        "data/futures/um/monthly/klines/BTCUSDT/1d/",
        "data/futures/um/monthly/klines/ETHUSDT/1d/",
    ]
    assert not any("ETHUSDT-1d-2024-01-" in file for file in archive.file_requests())

    again = archive.download(periods)
    assert again.downloaded == 0
    assert again.present == 4


def test_days_missing_from_monthly_klines_files_come_from_daily_files(
    archive: Archive,
) -> None:
    periods = klines_periods(
        datetime(2024, 3, 1, tzinfo=UTC), datetime(2024, 4, 1, tzinfo=UTC)
    )

    summary = archive.download(periods)

    # Both monthly files, BTC's 15th & ETH's 10th; BTC's 20th is in no file
    assert summary.downloaded == 4
    assert summary.missing == [
        PurePosixPath("futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-03-20.zip")
    ]
    assert (
        archive.root / "futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-03-15.zip"
    ).is_file()
    # BTC's 2 missing days are listed, ETH's single one is requested straight
    assert archive.listings() == ["data/futures/um/daily/klines/BTCUSDT/1d/"]
    assert "futures/um/daily/klines/ETHUSDT/1d/ETHUSDT-1d-2024-03-10.zip" in (
        archive.file_requests()
    )
    assert not any("2024-03-20" in file for file in archive.file_requests())

    # Local monthly files are kept as they are: not read again, nothing requested
    archive.server.requests.clear()
    again = archive.download(periods)
    assert again.downloaded == 0
    assert again.present == 2
    assert archive.server.requests == []


def test_known_missing_files_are_not_asked_for_again(archive: Archive) -> None:
    periods = klines_periods(
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 1, tzinfo=UTC)
    )
    first = archive.download(periods)

    archive.server.requests.clear()
    again = archive.download(periods, known_missing=first.missing)

    # ETH's January (its monthly file & its days) is missing without a request
    assert again.missing == first.missing
    assert archive.listings() == []
    assert not any("ETHUSDT" in file for file in archive.file_requests())
