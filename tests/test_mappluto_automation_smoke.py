"""Small offline implementation checks; comprehensive workflow tests are pending."""

from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import pytest
import json
from conftest import inspection_for
from pydantic import SecretStr

from housing_intelligence.config import Settings
from housing_intelligence.pipeline.mappluto.acquisition import RejectedArchive, acquire
from housing_intelligence.pipeline.mappluto.discovery import (
    ARCGIS,
    PUBLICATION,
    Discovery,
    OfficialHTTP,
    TransientError,
    approved_url,
    discover,
)
from housing_intelligence.pipeline.mappluto.quality import policy
from housing_intelligence.pipeline.mappluto.source import runtime_contract

URL = "https://s-media.nyc.gov/agencies/dcp/assets/mappluto_26v1_fgdb.zip"


def settings():
    return Settings(_env_file=None, database_password=SecretStr("offline-test"))


class Response(BytesIO):
    def __init__(self, payload=b"", headers=None):
        super().__init__(payload)
        self.headers = headers or {"Content-Type": "application/zip"}

    url = URL

    def iter_content(self, chunk_size):
        while chunk := self.read(chunk_size):
            yield chunk


def test_runtime_contract_accepts_minor_release_and_retains_crs_rule(contract):
    inspection = inspection_for(contract)
    dynamic = runtime_contract("25v2.1", inspection)
    assert dynamic.source_year == 2025 and dynamic.expected_count == 1
    inspection["layers"][0]["geometryFields"][0]["coordinateSystem"]["projjson"]["id"][
        "code"
    ] = 4326
    with pytest.raises(ValueError, match="EPSG:2263"):
        runtime_contract("25v2.1", inspection)


@pytest.mark.parametrize(
    "url",
    [
        "http://s-media.nyc.gov/agencies/dcp/a.zip",
        "https://s-media.nyc.gov.evil.example/agencies/dcp/a.zip",
        "https://s-media.nyc.gov/agencies/other/a.zip",
        "https://s-media.nyc.gov/agencies/dcp/a.shp",
    ],
)
def test_archive_provenance_rejects_unapproved_urls(url):
    with pytest.raises(ValueError):
        approved_url(url, archive=True)


def test_missing_anomaly_configuration_fails_closed():
    with pytest.raises(ValueError, match="explicitly configured"):
        policy(settings())


def test_discovery_uses_distinct_query_and_detects_inconsistent_snapshot(monkeypatch):
    http = OfficialHTTP(settings())
    query_urls = []
    metadata_calls = 0

    def fake_json(url):
        nonlocal metadata_calls
        query_urls.append(url)
        if url == PUBLICATION:
            publication = json.loads(
                (
                    Path(__file__).parent / "fixtures/mappluto/publication.json"
                ).read_bytes()
            )
            publication["description"] = publication["description"].replace(
                "26v2", "26v1"
            )
            return publication
        if "returnDistinctValues=true" in url:
            assert "returnGeometry=false" in url
            return {"features": [{"attributes": {"Version": "26v1"}}]}
        if "returnCountOnly=true" in url:
            return {"count": 10}
        assert url == ARCGIS + "?f=json"
        metadata_calls += 1
        return {"editingInfo": {"dataLastEditDate": metadata_calls}}

    monkeypatch.setattr(http, "json", fake_json)
    monkeypatch.setattr(http, "open", lambda *a, **kw: Response())
    with pytest.raises(TransientError, match="changed during discovery"):
        discover(http)
    assert len(query_urls) == 7


def test_acquisition_stores_complete_zip_and_preserves_checksum(tmp_path, monkeypatch):
    payload = BytesIO()
    with ZipFile(payload, "w") as archive:
        archive.writestr("source.gdb/table", "synthetic")
    body = payload.getvalue()
    http = OfficialHTTP(settings())
    monkeypatch.setattr(
        http,
        "open",
        lambda *a, **kw: Response(body, {"Content-Length": str(len(body))}),
    )
    source = Discovery("26v1", 1, 1, URL, URL, len(body), None, None)
    path, checksum, count = acquire(http, source, tmp_path)
    assert path == tmp_path / "26v1" / f"{checksum}.zip"
    assert path.read_bytes() == body and count == len(body)
    assert not list(tmp_path.rglob("*.part"))


def test_acquisition_rejects_invalid_zip_with_hash_diagnostics(tmp_path, monkeypatch):
    http = OfficialHTTP(settings())
    monkeypatch.setattr(http, "open", lambda *a, **kw: Response(b"invalid"))
    source = Discovery("26v1", 1, 1, URL, URL, None, None, None)
    with pytest.raises(RejectedArchive) as error:
        acquire(http, source, Path(tmp_path))
    assert len(error.value.checksum) == 64 and error.value.byte_count == 7
    assert not list(tmp_path.rglob("*.part"))
    assert not list(tmp_path.rglob("*.zip"))
