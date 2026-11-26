"""Offline official-publication and ArcGIS protocol coverage."""

import copy
import json
from io import BytesIO
from pathlib import Path
import requests
from unittest.mock import Mock

import pytest
from pydantic import SecretStr

from housing_intelligence.config import Settings
from housing_intelligence.pipeline.mappluto.discovery import (
    ARCGIS,
    PUBLICATION,
    OfficialHTTP,
    TransientError,
    approved_url,
    discover,
    publication_archive,
    retry,
)

FIXTURE = Path(__file__).parent / "fixtures/mappluto/publication.json"
URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/mappluto/nyc_mappluto_26v2_fgdb.zip"


def configured():
    return Settings(_env_file=None, database_password=SecretStr("offline"))


class Response(BytesIO):
    status_code = 200

    def __init__(self, body=b"", headers=None, url=URL):
        super().__init__(body)
        self.headers = headers or {
            "Content-Type": "application/zip",
            "Content-Length": "123",
        }
        self.url = url

    def iter_content(self, chunk_size):
        while chunk := self.read(chunk_size):
            yield chunk


@pytest.fixture
def metadata():
    return [
        {"editingInfo": {"dataLastEditDate": 1788967824444}},
        {"features": [{"attributes": {"Version": "26v2"}}]},
        {"count": 856687},
    ]


def fake_service(monkeypatch, metadata):
    client = OfficialHTTP(configured())

    def get(url):
        if url == PUBLICATION:
            return json.loads(FIXTURE.read_bytes())
        index = (
            1 if "returnDistinctValues" in url else 2 if "returnCountOnly" in url else 0
        )
        return copy.deepcopy(metadata[index])

    monkeypatch.setattr(client, "json", get)
    monkeypatch.setattr(client, "open", lambda *args, **kwargs: Response())
    return client


def test_captured_publication_selects_original_clipped_file():
    description = json.loads(FIXTURE.read_bytes())["description"]
    assert publication_archive(description, "26v2") == URL
    # A historical reference adjacent to the link must not change its release.
    description = description.replace(
        "<h3>MapPLUTO</h3>", "<h3>MapPLUTO</h3><p>Previously 25v2.1.</p>"
    )
    assert publication_archive(description, "26v2") == URL
    assert publication_archive(
        description.replace("26v2", "25v2.1"), "25v2.1"
    ).endswith("25v2.1_fgdb.zip")


@pytest.mark.parametrize(
    "defect", ["missing", "duplicate", "section", "version", "wrong_archive", "foreign"]
)
def test_publication_fails_explicitly(defect):
    description = json.loads(FIXTURE.read_bytes())["description"]
    if defect == "missing":
        description = description.replace(
            "MapPLUTO - Shoreline Clipped (fgdb)", "Renamed format"
        )
    elif defect == "duplicate":
        description = description.replace(URL, URL + '" data-extra="x')
        description = description.replace(
            "Download Files\n                              </a>",
            'Download Files</a><a href="' + URL + '">Download Files</a>',
        )
    elif defect == "section":
        description = description.replace('id="recent-release"', 'id="new-layout"')
    elif defect == "version":
        description = description.replace(
            "Latest Release: 26v2", "Latest Release: 26v1"
        )
    elif defect == "wrong_archive":
        description = description.replace(URL, URL.replace("26v2", "26v1"))
    else:
        description = description.replace(URL, "https://mirror.example/a.zip")
    with pytest.raises(ValueError):
        publication_archive(description, "26v2")


def test_discovery_exact_metadata(monkeypatch, metadata):
    result = discover(fake_service(monkeypatch, metadata))
    assert (result.version, result.edit_time, result.arcgis_count) == (
        "26v2",
        1788967824444,
        856687,
    )
    assert result.original_url == URL and result.content_length == 123


@pytest.mark.parametrize(
    "index,value",
    [
        (0, {}),
        (0, {"editingInfo": None}),
        (0, {"editingInfo": {"dataLastEditDate": True}}),
        (1, {"features": []}),
        (1, {"features": [{"attributes": {"Version": ""}}]}),
        (
            1,
            {
                "features": [
                    {"attributes": {"Version": "26v2"}},
                    {"attributes": {"Version": "26v1"}},
                ]
            },
        ),
        (1, {"features": [None]}),
        (1, {"exceededTransferLimit": True, "features": []}),
        (2, {"count": 0}),
        (2, {"count": "12"}),
        (2, {"count": False}),
    ],
)
def test_arcgis_rejects_malformed_or_mixed_responses(
    monkeypatch, metadata, index, value
):
    metadata[index] = value
    with pytest.raises(ValueError):
        discover(fake_service(monkeypatch, metadata))


def response(status=200, headers=None):
    result = Response(headers=headers)
    result.status_code = status
    return result


@pytest.mark.parametrize(
    "status,transient", [(404, False), (403, False), (429, True), (503, True)]
)
def test_http_error_classification(monkeypatch, status, transient):
    monkeypatch.setattr(requests, "request", Mock(return_value=response(status)))
    with pytest.raises(TransientError if transient else ValueError):
        OfficialHTTP(configured()).open(ARCGIS)


def test_redirect_is_checked_before_second_request(monkeypatch):
    request = Mock(
        return_value=response(302, {"Location": "https://evil.example/a.zip"})
    )
    monkeypatch.setattr(requests, "request", request)
    with pytest.raises(ValueError):
        OfficialHTTP(configured()).open(URL, archive=True)
    assert request.call_count == 1


def test_allowed_redirect_and_redirect_loop(monkeypatch):
    request = Mock(side_effect=[response(302, {"Location": URL}), response()])
    monkeypatch.setattr(requests, "request", request)
    with OfficialHTTP(configured()).open(URL, archive=True) as result:
        assert result.status_code == 200
    assert request.call_args.kwargs["allow_redirects"] is False
    assert request.call_args.kwargs["stream"] is True
    assert request.call_args.kwargs["timeout"] == (10, 60)
    request = Mock(side_effect=lambda *a, **kw: response(302, {"Location": URL}))
    monkeypatch.setattr(requests, "request", request)
    with pytest.raises(ValueError, match="Too many"):
        OfficialHTTP(configured()).open(URL, archive=True)
    assert request.call_count == 6


@pytest.mark.parametrize(
    "url",
    [
        "https://s-media.nyc.gov/agencies/dcp/../other/a.zip",
        "https://s-media.nyc.gov/agencies/dcp/%2e%2e/other/a.zip",
        "https://s-media.nyc.gov/agencies/dcp/%252e%252e/other/a.zip",
        "https://s-media.nyc.gov/agencies/dcp/%00a.zip",
        "https://s-media.nyc.gov/agencies/dcp/%0aa.zip",
        "https://s-media.nyc.gov/agencies/dcp/%5ca.zip",
        "https://user:secret@s-media.nyc.gov/agencies/dcp/a.zip",
        "https://s-media.nyc.gov:444/agencies/dcp/a.zip",
        "https://s-media.nyc.gov/agencies/dcp/a.zip#fragment",
        "https://s-media.nyc.gov/agencies/dcp/a.zip\n",
    ],
)
def test_url_edge_cases(url):
    with pytest.raises(ValueError):
        approved_url(url, archive=True)


@pytest.mark.parametrize(
    "body",
    [b"not json", b"[]", b'{"error":{"code":500}}', b"x" * 2_000_001],
    ids=["malformed", "list", "api_error", "oversized"],
)
def test_json_failures(monkeypatch, body):
    client = OfficialHTTP(configured())
    monkeypatch.setattr(client, "open", lambda *a, **kw: Response(body))
    with pytest.raises(ValueError):
        client.json(ARCGIS)


def test_transient_retries_are_bounded(monkeypatch):
    monkeypatch.setattr(
        "housing_intelligence.pipeline.mappluto.discovery.time.sleep", lambda _: None
    )
    operation = Mock(side_effect=TransientError("offline"))
    with pytest.raises(TransientError):
        retry(operation)
    assert operation.call_count == 3
    client = OfficialHTTP(configured())
    monkeypatch.setattr(
        requests, "request", Mock(side_effect=requests.ConnectionError("offline"))
    )
    with pytest.raises(TransientError):
        client.open(ARCGIS)


@pytest.mark.parametrize("version", ["26v2.1.2", "26v2garbage"])
def test_publication_version_cannot_match_only_a_prefix(version):
    description = json.loads(FIXTURE.read_bytes())["description"].replace(
        "Latest Release: 26v2", "Latest Release: " + version
    )
    with pytest.raises(ValueError):
        publication_archive(description, "26v2")
