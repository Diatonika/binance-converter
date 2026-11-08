from collections.abc import Iterator
from datetime import UTC, datetime
from functools import partial
from hashlib import sha256
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from threading import Thread

import pytest

from baikal.binance_converter.config import Config
from baikal.binance_converter.download.download import (
    ArchivePeriod,
    download_archives,
)
from baikal.binance_converter.enums import DataType, InstrumentType, Interval

FUTURES = InstrumentType.USD_M_FUTURES


def publish(archive: Path, file: str) -> None:
    """An archive file & its checksum file, as data.binance.vision serves them."""

    path = archive / "data" / file
    path.parent.mkdir(parents=True, exist_ok=True)
    content = f"content of {path.name}".encode()
    path.write_bytes(content)
    path.with_name(f"{path.name}.CHECKSUM").write_text(
        f"{sha256(content).hexdigest()}  {path.name}\n"
    )


@pytest.fixture
def archive(tmp_path: Path) -> Iterator[tuple[Path, str]]:
    """A local archive server: BTC has a monthly January file & daily February files,
    ETH a monthly February file only."""

    served = tmp_path / "served"
    btc = "futures/um/monthly/klines/BTCUSDT/1d/BTCUSDT-1d-2024-01.zip"
    publish(served, btc)
    for day in (1, 2):
        publish(
            served,
            f"futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-02-{day:02d}.zip",
        )
    publish(served, "futures/um/monthly/klines/ETHUSDT/1d/ETHUSDT-1d-2024-02.zip")

    handler = partial(SimpleHTTPRequestHandler, directory=str(served))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield tmp_path / "root", f"http://127.0.0.1:{server.server_port}/data"
    finally:
        server.shutdown()


def test_many_instruments_prefer_monthly_files_then_daily_ones(
    archive: tuple[Path, str],
) -> None:
    root, base_url = archive
    start, end = datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 2, 3, tzinfo=UTC)
    periods = [
        ArchivePeriod(
            Config(DataType.KLINES, FUTURES, symbol, Interval.ONE_DAY), start, end
        )
        for symbol in ("BTCUSDT", "ETHUSDT")
    ]

    summary = download_archives(root, periods, workers=4, base_url=base_url)

    assert summary.downloaded == 4  # BTC January & 2 February days, ETH February
    assert summary.missing == [
        # ETH has no January file, monthly or daily
        PurePosixPath("futures/um/daily/klines/ETHUSDT/1d/ETHUSDT-1d-2024-01-01.zip"),
        *(
            PurePosixPath(
                f"futures/um/daily/klines/ETHUSDT/1d/ETHUSDT-1d-2024-01-{day:02d}.zip"
            )
            for day in range(2, 32)
        ),
    ]
    assert (
        root / "futures/um/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-02-02.zip"
    ).is_file()

    again = download_archives(root, periods, workers=4, base_url=base_url)
    assert again.downloaded == 0
    assert again.present == 4
