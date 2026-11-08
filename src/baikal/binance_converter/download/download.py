from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from enum import Enum, auto
from hashlib import sha256
from http import HTTPStatus
from http.client import HTTPException
from pathlib import Path, PurePosixPath
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import quote
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

# Transient failures (timeouts, dropped connections, throttling & server errors) are
# retried with exponential backoff: 1, 2, 4, 8 seconds.
ATTEMPTS = 5
BACKOFF_SECONDS = 1.0
RETRIED_STATUSES = frozenset({HTTPStatus.TOO_MANY_REQUESTS}) | frozenset(
    status for status in HTTPStatus if status >= HTTPStatus.INTERNAL_SERVER_ERROR
)


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
@frozen
class ArchivePeriod:
    """Archive files of ``config`` covering [`start`, `end`)."""

    config: Config
    start: datetime
    end: datetime


@public
def download_archive(
    root: Path,
    config: Config,
    start: datetime,
    end: datetime,
    *,
    workers: int,
    base_url: str = ARCHIVE_URL,
    progress: bool = False,
) -> DownloadSummary:
    """Download archive files covering [`start`, `end`) into ``root`` directory, see
    `download_archives`."""

    return download_archives(
        root,
        [ArchivePeriod(config, start, end)],
        workers=workers,
        base_url=base_url,
        progress=progress,
    )


@public
def download_archives(
    root: Path,
    periods: Sequence[ArchivePeriod],
    *,
    workers: int,
    base_url: str = ARCHIVE_URL,
    progress: bool = False,
) -> DownloadSummary:
    """Download the archive files of many periods (e.g. of many instruments) into
    ``root`` directory, ``workers`` files at a time over all of them.

    Files keep their archive paths, as `load_klines` & other loaders expect. Monthly
    files are preferred, daily files replace monthly ones the archive lacks (e.g. of
    the current month). Local files are kept as is. ``progress`` shows a progress bar
    (e.g. from the command line).
    """

    # Monthly files of every period first, {file: (period index, month)}
    months = {
        archive_file(period.config, DataGranularity.MONTHLY, month): (index, month)
        for index, period in enumerate(periods)
        if DataGranularity.MONTHLY in period.config.data_type.granularities
        for month in file_dates(DataGranularity.MONTHLY, period.start, period.end)
    }
    states = download_files(
        root, list(months), workers=workers, base_url=base_url, progress=progress
    )

    daily: list[PurePosixPath] = []
    for index, period in enumerate(periods):
        granularities = period.config.data_type.granularities
        if DataGranularity.MONTHLY not in granularities:
            days = list(file_dates(DataGranularity.DAILY, period.start, period.end))
        elif DataGranularity.DAILY in granularities:
            # Daily files replace monthly ones the archive lacks, e.g. of the current month.
            missing_months = [
                month
                for file, (owner, month) in months.items()
                if owner == index and states[file] is FileState.MISSING
            ]
            days = days_of_months(missing_months, period.start, period.end)
        else:
            days = []

        daily += [
            archive_file(period.config, DataGranularity.DAILY, day) for day in days
        ]

    replaced = {
        file
        for file, (index, _) in months.items()
        if states[file] is FileState.MISSING
        and DataGranularity.DAILY in periods[index].config.data_type.granularities
    }
    states = {file: state for file, state in states.items() if file not in replaced}
    states |= download_files(
        root, daily, workers=workers, base_url=base_url, progress=progress
    )

    return DownloadSummary(
        present=sum(state is FileState.PRESENT for state in states.values()),
        downloaded=sum(state is FileState.DOWNLOADED for state in states.values()),
        missing=sorted(
            file for file, state in states.items() if state is FileState.MISSING
        ),
    )


@public
def archive_url(file: PurePosixPath, *, base_url: str = ARCHIVE_URL) -> str:
    """URL of the archive ``file``, its path percent-encoded (non-ASCII instruments)."""

    return f"{base_url}/{quote(file.as_posix())}"


@public
def download_file(root: Path, file: PurePosixPath, *, base_url: str) -> FileState:
    """Download archive ``file`` into ``root``, verifying its checksum."""

    destination = root / file
    if destination.is_file():
        return FileState.PRESENT

    url = archive_url(file, base_url=base_url)
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


@public
def download_files(
    root: Path,
    files: list[PurePosixPath],
    *,
    workers: int,
    base_url: str = ARCHIVE_URL,
    progress: bool = False,
) -> dict[PurePosixPath, FileState]:
    """Download archive ``files`` into ``root`` in parallel, e.g. files of a listing
    (see `list_files`) of many instruments at once; ``progress`` shows a progress bar."""

    with (
        Progress(transient=True, disable=not progress) as bar,
        ThreadPoolExecutor(max_workers=workers) as executor,
    ):
        task = bar.add_task("Download", total=len(files))

        def download(file: PurePosixPath) -> tuple[PurePosixPath, FileState]:
            state = download_file(root, file, base_url=base_url)
            bar.advance(task)
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


@public
def fetch(url: str) -> bytes:
    """Content of ``url``, retrying transient failures (see `ATTEMPTS`)."""

    for attempt in range(ATTEMPTS):
        try:
            with urlopen(url, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 - fixed https archive URL
                content: bytes = response.read()
                return content
        except (URLError, TimeoutError, ConnectionError, HTTPException) as error:
            if not is_transient(error) or attempt == ATTEMPTS - 1:
                raise

            sleep(BACKOFF_SECONDS * 2**attempt)

    message = f"No attempts to fetch {url}."
    raise AssertionError(message)


@private
def is_transient(error: OSError | HTTPException) -> bool:
    """Timeouts, dropped connections & truncated responses, throttling & server
    errors: not missing files."""

    if isinstance(error, HTTPError):
        return error.code in RETRIED_STATUSES

    return True
