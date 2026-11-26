"""Bounded, geometry-free discovery from the researched official interfaces."""

import json
import re
import time
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from urllib.parse import unquote, urlencode, urljoin, urlsplit
import requests

from ...config import Settings

ARCGIS = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/ArcGIS/rest/services/MAPPLUTO/FeatureServer/0"
PUBLICATION = "https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets/mappluto-pluto-change"
RELEASE = re.compile(r"(?<![\w.])(\d{2}v\d+(?:\.\d+)?)(?!\w|\.\d)", re.I)


class TransientError(RuntimeError):
    """An operation may succeed when repeated, subject to the attempt budget."""


def retry(operation):
    for attempt in range(3):
        try:
            return operation()
        except TransientError:
            if attempt == 2:
                raise
            time.sleep(2**attempt)


def approved_url(url: str, *, archive: bool = False) -> None:
    if any(ord(character) < 32 for character in url):
        raise ValueError("Control characters in source URL")
    parsed = urlsplit(url)
    decoded_path = unquote(parsed.path)
    hosts = {"s-media.nyc.gov"} if archive else {"services5.arcgis.com", "apps.nyc.gov"}
    if (
        parsed.scheme != "https"
        or parsed.hostname not in hosts
        or parsed.username
        or parsed.password
        or parsed.port not in (None, 443)
        or parsed.fragment
    ):
        raise ValueError("Unexpected source or redirect destination")
    if archive and (
        not parsed.path.startswith("/agencies/dcp/")
        or not parsed.path.lower().endswith(".zip")
        or any(part in {".", ".."} for part in decoded_path.split("/"))
        or "\\" in decoded_path
        or "%" in decoded_path
        or any(ord(character) < 32 for character in decoded_path)
    ):
        raise ValueError("Expected an official DCP ZIP path")


class OfficialHTTP:
    def __init__(self, settings: Settings):
        self.settings = settings

    def open(self, url: str, method: str = "GET", *, archive: bool = False):
        """Validate redirects before requesting them; use ordinary socket timeouts."""
        for _ in range(6):
            approved_url(url, archive=archive)
            try:
                response = requests.request(
                    method,
                    url,
                    stream=True,
                    allow_redirects=False,
                    timeout=(10, self.settings.mappluto_http_timeout_seconds),
                    headers={
                        "User-Agent": "NYCHousingIntelligence-MapPLUTO/1.0",
                        "Accept-Encoding": "identity",
                    },
                )
            except requests.RequestException as error:
                raise TransientError(
                    f"Source network error: {type(error).__name__}"
                ) from None
            if response.status_code == 200:
                return response
            status = response.status_code
            location = response.headers.get("Location")
            response.close()
            if status in {301, 302, 303, 307, 308} and location:
                url = urljoin(url, location)
                continue
            if status in {408, 429, 500, 502, 503, 504}:
                raise TransientError(f"Source HTTP {status}")
            raise ValueError(f"Source HTTP {status}")
        raise ValueError("Too many source redirects")

    def json(self, url: str) -> dict:
        try:
            with self.open(url) as response:
                payload = bytearray()
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    payload.extend(chunk)
                    if len(payload) > 2_000_000:
                        raise ValueError("Metadata response exceeds size limit")
            result = json.loads(payload)
        except requests.RequestException as error:
            raise TransientError(
                f"Metadata read failed: {type(error).__name__}"
            ) from None
        if not isinstance(result, dict) or "error" in result:
            raise ValueError("Expected successful JSON object from source")
        return result


class PublicationParser(HTMLParser):
    """Small HTML tree retaining the official release and table boundaries."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = {"tag": "root", "attrs": {}, "children": []}
        self.stack = [self.root]

    def handle_starttag(self, tag, attrs):
        node = {"tag": tag, "attrs": dict(attrs), "children": []}
        self.stack[-1]["children"].append(node)
        if tag not in {"br", "hr", "img", "input", "meta", "link", "source", "wbr"}:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if self.stack[-1]["tag"] == tag:
            self.stack.pop()

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index]["tag"] == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        self.stack[-1]["children"].append(data)


def nodes(node):
    yield node
    for child in node["children"]:
        if isinstance(child, dict):
            yield from nodes(child)


def node_text(node):
    return " ".join(
        node_text(child) if isinstance(child, dict) else child
        for child in node["children"]
    )


def publication_archive(description: str, version: str) -> str:
    parser = PublicationParser()
    parser.feed(description)
    sections = [
        node
        for node in nodes(parser.root)
        if node["attrs"].get("id") == "recent-release"
    ]
    if len(sections) != 1:
        raise ValueError("Expected one official recent-release section")
    section = sections[0]
    declared = set(
        re.findall(
            r"latest\s+release\s*:\s*" + RELEASE.pattern, node_text(section), re.I
        )
    )
    if {item.lower() for item in declared} != {version}:
        raise ValueError(
            "Latest publication and ArcGIS release disagree or are ambiguous"
        )
    map_sections = [
        node for node in nodes(section) if node["attrs"].get("id") == "mappluto"
    ]
    if len(map_sections) != 1:
        raise ValueError("Expected one MapPLUTO subsection")
    matches = []
    for table in [node for node in nodes(map_sections[0]) if node["tag"] == "table"]:
        selected = False
        for row in [node for node in nodes(table) if node["tag"] == "tr"]:
            headings = [node for node in nodes(row) if node["tag"] == "th"]
            if headings:
                label = re.sub(r"\s+", " ", " ".join(map(node_text, headings))).strip()
                selected = (
                    label.replace("?", "-").replace("?", "-").casefold()
                    == "mappluto - shoreline clipped (fgdb)"
                )
            if selected:
                for anchor in [node for node in nodes(row) if node["tag"] == "a"]:
                    if (
                        re.sub(r"\s+", " ", node_text(anchor)).strip().casefold()
                        == "download files"
                    ):
                        matches.append(anchor["attrs"].get("href", ""))
    if len(matches) != 1:
        raise ValueError("Expected exactly one shoreline-clipped FileGDB download")
    href = matches[0]
    approved_url(href, archive=True)
    named = RELEASE.findall(urlsplit(href).path.replace("_", " "))
    if named and any(item.lower() != version for item in named):
        raise ValueError("Published archive filename has a conflicting version")
    return href


@dataclass(frozen=True)
class Discovery:
    version: str
    edit_time: int
    arcgis_count: int
    original_url: str
    final_url: str
    content_length: int | None
    etag: str | None
    last_modified: str | None

    def asdict(self):
        return asdict(self)


def content_length(headers) -> int | None:
    value = headers.get("Content-Length")
    if value is None:
        return None
    if not re.fullmatch(r"[0-9]+", value) or int(value) <= 0:
        raise ValueError("Invalid advertised file size")
    return int(value)


def discover(http: OfficialHTTP) -> Discovery:
    def arcgis():
        metadata = http.json(ARCGIS + "?f=json")
        versions = http.json(
            ARCGIS
            + "/query?"
            + urlencode(
                {
                    "where": "1=1",
                    "outFields": "Version",
                    "returnDistinctValues": "true",
                    "returnGeometry": "false",
                    "f": "json",
                }
            )
        )
        counts = http.json(
            ARCGIS
            + "/query?"
            + urlencode(
                {
                    "where": "1=1",
                    "returnCountOnly": "true",
                    "f": "json",
                }
            )
        )
        features = versions.get("features")
        if (
            versions.get("exceededTransferLimit")
            or not isinstance(features, list)
            or len(features) != 1
        ):
            raise ValueError("Expected one complete distinct-version result")
        feature = features[0]
        if not isinstance(feature, dict) or not isinstance(
            feature.get("attributes"), dict
        ):
            raise ValueError("Invalid ArcGIS version attributes")
        editing = metadata.get("editingInfo")
        if not isinstance(editing, dict):
            raise ValueError("Missing ArcGIS editing metadata")
        version = feature["attributes"].get("Version")
        edited = editing.get("dataLastEditDate")
        count = counts.get("count")
        if not isinstance(version, str) or not RELEASE.fullmatch(version):
            raise ValueError("Missing or invalid unique ArcGIS version")
        if (
            type(edited) is not int
            or edited <= 0
            or type(count) is not int
            or count <= 0
        ):
            raise ValueError("Missing or invalid ArcGIS edit time/count")
        return version.lower(), edited, count

    version, edited, count = arcgis()
    publication = http.json(PUBLICATION)
    description = publication.get("description")
    if not isinstance(description, str) or not description:
        raise ValueError("Missing NYC Planning publication description")
    archive_url = publication_archive(description, version)
    with http.open(archive_url, "HEAD", archive=True) as response:
        final_url = response.url
        approved_url(final_url, archive=True)
        kind = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
        if kind not in {
            "application/zip",
            "application/x-zip-compressed",
            "application/octet-stream",
        }:
            raise ValueError("Archive HEAD does not describe ZIP/binary content")
        length = content_length(response.headers)
        if length and length > http.settings.mappluto_max_archive_bytes:
            raise ValueError("Advertised archive exceeds configured size limit")
        result = Discovery(
            version,
            edited,
            count,
            archive_url,
            final_url,
            length,
            response.headers.get("ETag"),
            response.headers.get("Last-Modified"),
        )
    # Detect publication during the independent API requests; do not combine snapshots.
    if arcgis() != (version, edited, count):
        raise TransientError("ArcGIS changed during discovery")
    return result
