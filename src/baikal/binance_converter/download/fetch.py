"""Requests to the **binance.vision** archive (files & listings), retrying transient
failures."""

from http import HTTPStatus
from http.client import HTTPException
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from public import public

TIMEOUT_SECONDS = 60

# Transient failures (timeouts, dropped connections, throttling & server errors) are
# retried with exponential backoff: 1, 2, 4, 8 seconds.
ATTEMPTS = 5
BACKOFF_SECONDS = 1.0
RETRIED_STATUSES = frozenset({HTTPStatus.TOO_MANY_REQUESTS}) | frozenset(
    status for status in HTTPStatus if status >= HTTPStatus.INTERNAL_SERVER_ERROR
)


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


@public
def is_transient(error: OSError | HTTPException) -> bool:
    """Timeouts, dropped connections & truncated responses, throttling & server
    errors: not missing files."""

    if isinstance(error, HTTPError):
        return error.code in RETRIED_STATUSES

    return True
