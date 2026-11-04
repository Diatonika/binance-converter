"""Listing of the **binance.vision** archive: published instruments & files.

The archive is an S3 bucket: its listing API returns up to 1000 keys or common
prefixes (folders) per page, continued from the last key of the previous page.
"""

import re

from collections.abc import Iterator
from datetime import date
from pathlib import PurePosixPath
from urllib.parse import urlencode
from xml.etree.ElementTree import Element, fromstring

from attrs import frozen
from public import private, public

from baikal.binance_converter._data_granularity import DataGranularity
from baikal.binance_converter.archive import archive_folder
from baikal.binance_converter.config import Config
from baikal.binance_converter.download.download import fetch
from baikal.binance_converter.enums import DataType, InstrumentType

BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

# Keys of the bucket are archive paths under this prefix.
DATA_PREFIX = "data/"

S3_NAMESPACE = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

CHECKSUM_SUFFIX = ".CHECKSUM"

FILE_DATE_PATTERN = {
    DataGranularity.DAILY: re.compile(r"-(\d{4})-(\d{2})-(\d{2})\.zip$"),
    DataGranularity.MONTHLY: re.compile(r"-(\d{4})-(\d{2})\.zip$"),
}


@public
@frozen
class ArchiveFile:
    """Published archive file: path relative to the archive root, as `archive_file`
    builds it, and its size in bytes."""

    path: PurePosixPath
    size: int

    def file_date(self, granularity: DataGranularity) -> date:
        """Date of the file: the day of a daily file, the 1st of a monthly file's
        month."""

        return parse_file_date(self.path.name, granularity)


@public
@frozen
class ListingPage:
    """Page of the bucket listing: folders (with the delimiter) & files."""

    prefixes: list[str]
    files: list[ArchiveFile]
    next_marker: str | None  # Continues the listing, None on the last page


@public
def list_instruments(
    instrument_type: InstrumentType,
    granularity: DataGranularity,
    data_type: DataType,
    *,
    bucket_url: str = BUCKET_URL,
) -> list[str]:
    """Instruments the archive publishes files of, e.g. every USD-M futures symbol
    with monthly KLines."""

    prefix = f"{DATA_PREFIX}{instrument_type}/{granularity}/{data_type}/"

    return [
        folder.removeprefix(prefix).rstrip("/")
        for page in list_pages(prefix, delimiter=True, bucket_url=bucket_url)
        for folder in page.prefixes
    ]


@public
def list_files(
    config: Config, granularity: DataGranularity, *, bucket_url: str = BUCKET_URL
) -> list[ArchiveFile]:
    """Archive files of ``config`` (without checksum files), ordered by date. Files
    without a date of the granularity are skipped: the archive has a few strays (e.g.
    `MATICUSDT-metrics-1993.zip` among the daily metrics files)."""

    prefix = f"{DATA_PREFIX}{archive_folder(config, granularity)}/"
    pattern = FILE_DATE_PATTERN[granularity]

    files = [
        file
        for page in list_pages(prefix, delimiter=False, bucket_url=bucket_url)
        for file in page.files
        if pattern.search(file.path.name)
    ]

    return sorted(files, key=lambda file: file.file_date(granularity))


@public
def list_pages(
    prefix: str, *, delimiter: bool, bucket_url: str = BUCKET_URL
) -> Iterator[ListingPage]:
    """Pages of the bucket keys under ``prefix``: folders below it with the
    ``delimiter``, all files otherwise."""

    marker = ""
    while True:
        query = {"prefix": prefix, "marker": marker} | (
            {"delimiter": "/"} if delimiter else {}
        )

        page = parse_listing(fetch(f"{bucket_url}?{urlencode(query)}"))
        yield page

        if page.next_marker is None:
            return

        marker = page.next_marker


@public
def parse_listing(content: bytes) -> ListingPage:
    """Page of the S3 `ListBucketResult` XML document."""

    root = fromstring(content)  # noqa: S314 - XML of the public archive bucket

    prefixes = [
        text(element)
        for element in root.findall("s3:CommonPrefixes/s3:Prefix", S3_NAMESPACE)
    ]
    files = [
        ArchiveFile(
            path=PurePosixPath(key.removeprefix(DATA_PREFIX)),
            size=int(text(child(item, "Size"))),
        )
        for item in root.findall("s3:Contents", S3_NAMESPACE)
        if not (key := text(child(item, "Key"))).endswith(CHECKSUM_SUFFIX)
    ]

    truncated = text(child(root, "IsTruncated")) == "true"
    if not truncated:
        return ListingPage(prefixes=prefixes, files=files, next_marker=None)

    # Delimited listings report the next marker, plain ones continue from the last
    # key of the page.
    next_marker = root.findtext("s3:NextMarker", None, S3_NAMESPACE)
    if next_marker is None:
        keys = [
            text(child(item, "Key"))
            for item in root.findall("s3:Contents", S3_NAMESPACE)
        ]
        next_marker = max([*keys, *prefixes])

    return ListingPage(prefixes=prefixes, files=files, next_marker=next_marker)


@public
def parse_file_date(name: str, granularity: DataGranularity) -> date:
    """Date of an archive file name, e.g. ``BTCUSDT-1m-2020-01.zip``."""

    match = FILE_DATE_PATTERN[granularity].search(name)
    if match is None:
        message = f"File name '{name}' has no {granularity} date."
        raise ValueError(message)

    year, month, *day = (int(group) for group in match.groups())
    return date(year, month, day[0] if day else 1)


@private
def child(element: Element, tag: str) -> Element:
    found = element.find(f"s3:{tag}", S3_NAMESPACE)
    if found is None:
        message = f"Listing element <{element.tag}> has no <{tag}>."
        raise ValueError(message)

    return found


@private
def text(element: Element) -> str:
    return element.text or ""
