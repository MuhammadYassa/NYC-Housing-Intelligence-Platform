"""Stream immutable, content-addressed bronze artifacts from verified sources."""

import hashlib
import logging
import os
import tempfile
import requests
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from .bronze import calculate_sha256
from .discovery import Discovery, OfficialHTTP, TransientError, content_length
from .errors import InvalidArtifact, OperationalError


class RejectedArchive(InvalidArtifact):
    def __init__(self, message: str, checksum: str, byte_count: int):
        self.checksum = checksum
        self.byte_count = byte_count
        super().__init__(message)


def acquire(http: OfficialHTTP, source: Discovery, root: Path) -> tuple[Path, str, int]:
    directory = root.resolve() / source.version
    directory.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix="download-", suffix=".part", dir=directory
    )
    temporary = Path(name)
    digest = hashlib.sha256()
    received = 0
    downloaded = False
    try:
        with os.fdopen(descriptor, "wb") as output:
            with http.open(source.original_url, archive=True) as response:
                if response.url != source.final_url:
                    raise TransientError("Archive destination changed after HEAD")
                headers = response.headers
                length = content_length(headers)
                for key, expected in (
                    ("ETag", source.etag),
                    ("Last-Modified", source.last_modified),
                ):
                    if expected is not None and headers.get(key) != expected:
                        raise TransientError("Archive headers changed after HEAD")
                if (
                    source.content_length is not None
                    and length is not None
                    and length != source.content_length
                ):
                    raise TransientError("Archive size changed after HEAD")
                if headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise ValueError("Unexpected archive content encoding")
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    received += len(chunk)
                    if received > http.settings.mappluto_max_archive_bytes:
                        raise ValueError("Download exceeds configured archive limit")
                    digest.update(chunk)
                    output.write(chunk)
                if received == 0 or any(
                    size is not None and received != size
                    for size in (length, source.content_length)
                ):
                    raise TransientError(
                        "Incomplete download: received size disagrees with headers"
                    )
            output.flush()
            os.fsync(output.fileno())
        downloaded = True
        try:
            with ZipFile(temporary) as archive:
                entries = archive.infolist()
                if (
                    not entries
                    or sum(entry.file_size for entry in entries)
                    > http.settings.mappluto_max_expanded_bytes
                ):
                    raise ValueError(
                        "Empty archive or expanded ZIP exceeds configured limit"
                    )
                if any(entry.flag_bits & 1 for entry in entries):
                    raise ValueError("Encrypted source ZIP is not supported")
                if archive.testzip() is not None:
                    raise ValueError("ZIP member checksum failed")
        except BadZipFile as error:
            raise ValueError("Downloaded artifact is not a valid ZIP") from error
        checksum = digest.hexdigest().upper()
        final = directory / f"{checksum}.zip"
        if final.exists():
            if calculate_sha256(final) != checksum:
                raise OperationalError(
                    "Existing bronze artifact has been modified; restore it before retrying"
                )
        else:
            # Exclusive creation prevents replacing an accepted artifact. The
            # hard link exposes only the fully written, fsynced temporary file.
            os.link(temporary, final)
        return final, checksum, received
    except ValueError as error:
        if downloaded:
            raise RejectedArchive(
                str(error), digest.hexdigest().upper(), received
            ) from error
        raise
    except (requests.RequestException, TimeoutError, OSError) as error:
        raise TransientError(
            f"Acquisition I/O failure: {type(error).__name__}"
        ) from None
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            logging.getLogger(__name__).warning(
                "Could not remove incomplete download %s", temporary
            )
