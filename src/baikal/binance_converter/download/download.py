from collections.abc import Collection, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from enum import Enum, auto
from hashlib import sha256
from http import HTTPStatus
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError
from urllib.parse import quote

from attrs import frozen
from dateutil.relativedelta import relativedelta
from public import private, public
from rich.progress import Progress

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.archive import archive_file, file_dates
from baikal.binance_converter.config import Config
from baikal.binance_converter.download.fetch import fetch
from baikal.binance_converter.download.listing import BUCKET_URL, published_files
from baikal.binance_converter.klines.klines import load_from_zip

ARCHIVE_URL = "https://data.binance.vision/data"


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
    # Not published by the archive (with the monthly files the daily ones replace)
    missing: list[PurePosixPath]


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
    bucket_url: str = BUCKET_URL,
    known_missing: Collection[PurePosixPath] = (),
    progress: bool = False,
) -> DownloadSummary:
    """Download archive files covering [`start`, `end`) into ``root`` directory, see
    `download_archives`."""

    return download_archives(
        root,
        [ArchivePeriod(config, start, end)],
        workers=workers,
        base_url=base_url,
        bucket_url=bucket_url,
        known_missing=known_missing,
        progress=progress,
    )


@public
def download_archives(
    root: Path,
    periods: Sequence[ArchivePeriod],
    *,
    workers: int,
    base_url: str = ARCHIVE_URL,
    bucket_url: str = BUCKET_URL,
    known_missing: Collection[PurePosixPath] = (),
    progress: bool = False,
) -> DownloadSummary:
    """Download the archive files of many periods (e.g. of many instruments) into
    ``root`` directory, ``workers`` files at a time over all of them.

    Files keep their archive paths, as `load_klines` & other loaders expect. Monthly
    files are preferred. Daily files replace the monthly ones the archive lacks (e.g. of
    the current month) and fill the days a monthly KLines file has no rows of (the
    archive misses days in some of them, e.g. 2022-04-01 & 02): those of the monthly
    files downloaded now, local files are kept as is. ``known_missing``: files an earlier
    call found missing (see `DownloadSummary.missing`), not asked for again; the caller
    knows which of them the archive may still publish. ``progress`` shows a progress bar
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
        root,
        list(months),
        workers=workers,
        base_url=base_url,
        bucket_url=bucket_url,
        known_missing=known_missing,
        progress=progress,
    )

    daily: list[PurePosixPath] = []
    for index, period in enumerate(periods):
        granularities = period.config.data_type.granularities
        if DataGranularity.MONTHLY not in granularities:
            days = list(file_dates(DataGranularity.DAILY, period.start, period.end))
        elif DataGranularity.DAILY in granularities:
            own = [
                (file, month)
                for file, (owner, month) in months.items()
                if owner == index
            ]
            missing_months = [
                month for file, month in own if states[file] is FileState.MISSING
            ]
            days = days_of_months(missing_months, period.start, period.end)
            if period.config.data_type.is_klines:
                days += [
                    day
                    for file, month in own
                    if states[file] is FileState.DOWNLOADED
                    for day in days_without_klines(root / file, period, month)
                ]
        else:
            days = []

        daily += [
            archive_file(period.config, DataGranularity.DAILY, day) for day in days
        ]

    states |= download_files(
        root,
        daily,
        workers=workers,
        base_url=base_url,
        bucket_url=bucket_url,
        known_missing=known_missing,
        progress=progress,
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
    bucket_url: str = BUCKET_URL,
    known_missing: Collection[PurePosixPath] = (),
    progress: bool = False,
) -> dict[PurePosixPath, FileState]:
    """Download archive ``files`` into ``root`` in parallel, e.g. files of a listing
    (see `list_files`) of many instruments at once; ``progress`` shows a progress bar.

    Only the files the archive publishes are requested (see `published_files`): a
    folder missing many files locally is listed first. ``known_missing`` files are
    missing without a request."""

    missing = set(known_missing)
    states = {file: FileState.PRESENT for file in files if (root / file).is_file()}
    states |= {
        file: FileState.MISSING
        for file in files
        if file not in states and file in missing
    }
    absent = [file for file in files if file not in states]
    published = published_files(absent, workers=workers, bucket_url=bucket_url)
    states |= {file: FileState.MISSING for file in absent if file not in published}
    requested = [file for file in absent if file in published]

    with (
        Progress(transient=True, disable=not progress) as bar,
        ThreadPoolExecutor(max_workers=workers) as executor,
    ):
        task = bar.add_task("Download", total=len(requested))

        def download(file: PurePosixPath) -> tuple[PurePosixPath, FileState]:
            state = download_file(root, file, base_url=base_url)
            bar.advance(task)
            return file, state

        states |= dict(executor.map(download, requested))

    return states


@private
def days_without_klines(path: Path, period: ArchivePeriod, month: date) -> list[date]:
    """Days of the period within ``month`` the monthly KLines file has no rows of."""

    klines = load_from_zip(path, period.config.instrument_type).collect()
    present = set(klines["date_time"].dt.date().to_list())
    return [
        day
        for day in days_of_months([month], period.start, period.end)
        if day not in present
    ]


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
