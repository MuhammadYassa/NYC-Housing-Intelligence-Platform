"""Synthetic archives and local HTTP peers; no live archive requests."""

import hashlib
from io import BytesIO
from unittest.mock import Mock
from zipfile import ZipFile

import pytest
from pydantic import SecretStr

from housing_intelligence.config import Settings
from housing_intelligence.pipeline.mappluto.acquisition import acquire, RejectedArchive
from housing_intelligence.pipeline.mappluto.discovery import (
    Discovery,
    OfficialHTTP,
    TransientError,
)
from housing_intelligence.pipeline.mappluto.errors import OperationalError

URL = "https://s-media.nyc.gov/agencies/dcp/source.zip"


class Response(BytesIO):
    def __init__(self, body, headers=None, url=URL):
        super().__init__(body)
        self.headers = headers or {}
        self.url = url

    def iter_content(self, chunk_size):
        while chunk := self.read(chunk_size):
            yield chunk


def archive_bytes(marker=b"synthetic data"):
    buffer = BytesIO()
    with ZipFile(buffer, "w") as output:
        output.writestr("source.gdb/table", marker)
    return buffer.getvalue()


def client(body, headers=None, **changes):
    settings = Settings(_env_file=None, database_password=SecretStr("offline"))
    settings = settings.model_copy(update=changes)
    http = OfficialHTTP(settings)
    http.open = Mock(side_effect=lambda *a, **kw: Response(body, headers))
    return http


def source(length=None, etag=None):
    return Discovery("26v2", 1, 100, URL, URL, length, etag, None)


def test_streamed_hash_storage_reuse_and_same_version_correction(tmp_path):
    first = archive_bytes()
    first_path, checksum, size = acquire(client(first), source(len(first)), tmp_path)
    assert checksum == hashlib.sha256(first).hexdigest().upper()
    assert first_path.read_bytes() == first and size == len(first)
    assert acquire(client(first), source(), tmp_path)[0] == first_path
    revised = archive_bytes(b"corrected")
    revised_path, revised_sha, _ = acquire(client(revised), source(), tmp_path)
    assert revised_path != first_path and revised_sha != checksum
    assert first_path.read_bytes() == first
    assert not list(tmp_path.rglob("*.part"))


@pytest.mark.parametrize(
    "problem", ["length", "etag", "encoding", "destination", "empty"]
)
def test_inconsistent_download_never_finalizes(tmp_path, problem):
    body = archive_bytes()
    headers = {}
    selected = source()
    http = client(body)
    expected = TransientError
    if problem == "length":
        selected = source(len(body) + 1)
    elif problem == "etag":
        selected = source(etag="old")
        headers["ETag"] = "new"
    elif problem == "encoding":
        headers["Content-Encoding"] = "gzip"
        expected = ValueError
    elif problem == "destination":
        http.open = Mock(
            side_effect=lambda *a, **kw: Response(
                body, url="https://evil.example/a.zip"
            )
        )
    else:
        body = b""
    if problem != "destination":
        http.open = Mock(side_effect=lambda *a, **kw: Response(body, headers))
    with pytest.raises(expected):
        acquire(http, selected, tmp_path)
    assert not list(tmp_path.rglob("*.zip")) and not list(tmp_path.rglob("*.part"))


@pytest.mark.parametrize("problem", ["not_zip", "crc", "expanded", "download_cap"])
def test_bad_artifacts_are_rejected(tmp_path, problem):
    body = archive_bytes()
    changes = {}
    error = RejectedArchive
    if problem == "not_zip":
        body = b"not a zip"
    elif problem == "crc":
        body = body.replace(b"synthetic data", b"corrupted data")
    elif problem == "expanded":
        changes["mappluto_max_expanded_bytes"] = 1
    else:
        changes["mappluto_max_archive_bytes"] = 1
        error = ValueError
    with pytest.raises(error) as rejected:
        acquire(client(body, **changes), source(), tmp_path)
    if isinstance(rejected.value, RejectedArchive):
        assert rejected.value.checksum == hashlib.sha256(body).hexdigest().upper()
    assert not list(tmp_path.rglob("*.zip"))


def test_existing_corrupted_bronze_file_is_not_overwritten(tmp_path):
    body = archive_bytes()
    path = tmp_path / "26v2" / (hashlib.sha256(body).hexdigest().upper() + ".zip")
    path.parent.mkdir()
    path.write_bytes(b"operator must restore this file")
    with pytest.raises(OperationalError):
        acquire(client(body), source(), tmp_path)
    assert path.read_bytes() == b"operator must restore this file"


def test_local_disk_failure_is_retryable_and_partial_removed(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "housing_intelligence.pipeline.mappluto.acquisition.os.fsync",
        Mock(side_effect=OSError("disk unavailable")),
    )
    with pytest.raises(TransientError):
        acquire(client(archive_bytes()), source(), tmp_path)
    assert not list(tmp_path.rglob("*.part"))


@pytest.mark.parametrize("error_name", ["ReadTimeout", "ChunkedEncodingError"])
def test_interrupted_stream_removes_partial_download(tmp_path, error_name):
    import requests

    http = client(archive_bytes())
    response = Response(b"unused")

    def interrupted(chunk_size):
        yield b"partial archive"
        raise getattr(requests.exceptions, error_name)("interrupted source")

    response.iter_content = interrupted
    http.open = Mock(return_value=response)
    with pytest.raises(TransientError, match="I/O failure"):
        acquire(http, source(), tmp_path)
    assert not list(tmp_path.rglob("*.part"))
    assert not list(tmp_path.rglob("*.zip"))
