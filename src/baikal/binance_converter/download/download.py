from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from enum import Enum, auto
from hashlib import sha256
from http import HTTPStatus
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError
from urllib.request import urlopen

from attrs import frozen
from dateutil.relativedelta import relativedelta
from public import private, public
from rich.progress import Progress

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.archive import archive_file, file_dates
from baikal.binance_converter.config import Config

ARCHIVE_URL = "https://data.binance.vision/data"

TIMEOUT_SECONDS = 60


@public
class FileState(Enum):
    """Download outcome of an archive file."""

    PRESENT = auto()  # Found locally, not downloaded
    DOWNLOADED = auto()
    MISSING = auto()  # Not published by the archive


@public
@frozen
class DownloadSummary:
    present: int  # Found locally, not downloaded
    downloaded: int
    missing: list[PurePosixPath]  # Not published by the archive


@public
def download_archive(
    root: Path,
    config: Config,
    start: datetime,
    end: datetime,
    *,
    workers: int,
    base_url: str = ARCHIVE_URL,
) -> DownloadSummary:
    """Download archive files covering [`start`, `end`) into ``root`` directory.

    Files keep their archive paths, as `load_klines` & other loaders expect. Monthly
    files are preferred, daily files replace monthly ones the archive lacks (e.g. of
    the current month). Local files are kept as is.
    """

    granularities = config.data_type.granularities

    states: dict[PurePosixPath, FileState] = {}
    days: list[date] = []

    if DataGranularity.MONTHLY in granularities:
        months = {
            archive_file(config, DataGranularity.MONTHLY, month): month
            for month in file_dates(DataGranularity.MONTHLY, start, end)
        }

        states = download_files(root, list(months), workers=workers, base_url=base_url)

        if DataGranularity.DAILY in granularities:
            # Daily files replace monthly ones the archive lacks, e.g. of the current month.
            missing_months = [
                months[file]
                for file, state in states.items()
                if state is FileState.MISSING
            ]

            states = {
                file: state
                for file, state in states.items()
                if state is not FileState.MISSING
            }

            days = days_of_months(missing_months, start, end)
    else:
        days = list(file_dates(DataGranularity.DAILY, start, end))

    daily = [archive_file(config, DataGranularity.DAILY, day) for day in days]
    states |= download_files(root, daily, workers=workers, base_url=base_url)

    return DownloadSummary(
        present=sum(state is FileState.PRESENT for state in states.values()),
        downloaded=sum(state is FileState.DOWNLOADED for state in states.values()),
        missing=sorted(
            file for file, state in states.items() if state is FileState.MISSING
        ),
    )


@public
def download_file(root: Path, file: PurePosixPath, *, base_url: str) -> FileState:
    """Download archive ``file`` into ``root``, verifying its checksum."""

    destination = root / file
    if destination.is_file():
        return FileState.PRESENT

    url = f"{base_url}/{file}"
    try:
        content = fetch(url)
    except HTTPError as error:
        if error.code == HTTPStatus.NOT_FOUND:
            return FileState.MISSING
        raise

    expected = parse_checksum(fetch(f"{url}.CHECKSUM"))
    if sha256(content).hexdigest() != expected:
        message = f"Checksum mismatch of {url}."
        raise ValueError(message)

    # Written under a temporary name first: an interrupted download leaves no file.
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f"{destination.name}.part")
    partial.write_bytes(content)
    partial.replace(destination)

    return FileState.DOWNLOADED


# content: `<sha256 hex digest>  <file name>`
@public
def parse_checksum(content: bytes) -> str:
    return content.decode("utf-8").split()[0].lower()


@private
def download_files(
    root: Path, files: list[PurePosixPath], *, workers: int, base_url: str
) -> dict[PurePosixPath, FileState]:
    with (
        Progress(transient=True) as progress,
        ThreadPoolExecutor(max_workers=workers) as executor,
    ):
        task = progress.add_task("Download", total=len(files))

        def download(file: PurePosixPath) -> tuple[PurePosixPath, FileState]:
            state = download_file(root, file, base_url=base_url)
            progress.advance(task)
            return file, state

        return dict(executor.map(download, files))


@private
def days_of_months(
    months: Iterable[date], start: datetime, end: datetime
) -> list[date]:
    """Days of ``months`` within [`start`, `end`)."""

    days: list[date] = []
    for month in months:
        month_start = datetime.combine(month, time(), tzinfo=start.tzinfo)
        month_end = month_start + relativedelta(months=1)

        days.extend(
            file_dates(
                DataGranularity.DAILY, max(start, month_start), min(end, month_end)
            )
        )

    return days


@private
def fetch(url: str) -> bytes:
    with urlopen(url, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 - fixed https archive URL
        content: bytes = response.read()
        return content
