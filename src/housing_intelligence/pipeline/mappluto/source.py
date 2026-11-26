"""Independent compatibility rules and manual/runtime release descriptors."""

from dataclasses import dataclass
import re

# Only fields consumed by the canonical mapping are required. GDAL lowercases
# them during staging; numeric categories also define acceptable staging types.
REQUIRED_FIELDS = {
    "bbl": "Real",
    "borocode": "Integer",
    "borough": "String",
    "block": "Integer",
    "lot": "Integer",
    "address": "String",
    "zipcode": "Integer",
    "cd": "Integer",
    "bldgclass": "String",
    "landuse": "String",
    "ownername": "String",
    "lotarea": "Integer",
    "bldgarea": "Integer",
    "numbldgs": "Integer",
    "numfloors": "Real",
    "unitsres": "Integer",
    "unitstotal": "Integer",
    "yearbuilt": "Integer",
    "latitude": "Real",
    "longitude": "Real",
    "version": "String",
}
STAGING_TYPES = {
    "Integer": {"int2", "int4", "int8"},
    "Real": {"float4", "float8", "numeric"},
    "String": {"text", "varchar", "bpchar"},
}


@dataclass(frozen=True)
class SourceContract:
    release: str
    layer: str
    source_year: int
    expected_count: int
    srid: int = 2263


# Reviewed contracts for existing manual imports.
# Automated imports use runtime_contract() to support new releases.
CONTRACTS = {
    "26v1": SourceContract("26v1", "MapPLUTO_26v1_clipped", 2026, 856614),
}


def contract_for_release(release: str) -> SourceContract:
    try:
        return CONTRACTS[release]
    except KeyError:
        raise ValueError(
            f"Unsupported MapPLUTO release {release!r}; add a reviewed source contract first"
        ) from None


def runtime_contract(release: str, inspection: dict) -> SourceContract:
    """Measure a release while retaining the independent compatibility rules."""
    if not re.fullmatch(r"\d{2}v\d+(?:\.\d+)?", release):
        raise ValueError("Invalid MapPLUTO release identifier")
    layers = inspection.get("layers", [])
    # Do not silently choose among multiple potentially different snapshots.
    if len(layers) != 1:
        raise ValueError("Expected exactly one source feature layer")
    layer = layers[0]
    if not isinstance(layer.get("name"), str) or not layer["name"].strip():
        raise ValueError("Missing source layer name")
    contract = SourceContract(
        release, layer["name"], 2000 + int(release[:2]), layer.get("featureCount")
    )
    validate_inspection(inspection, contract)
    return contract


def validate_inspection(inspection: dict, contract: SourceContract) -> int:
    if inspection.get("driverShortName") != "OpenFileGDB":
        raise ValueError("Expected the OpenFileGDB source driver")
    layers = [
        layer
        for layer in inspection.get("layers", [])
        if layer.get("name") == contract.layer
    ]
    if len(layers) != 1:
        raise ValueError(f"Expected exactly one layer named {contract.layer}")
    layer = layers[0]
    fields = {field["name"].lower(): field["type"] for field in layer.get("fields", [])}
    if len(fields) != len(layer.get("fields", [])):
        raise ValueError("Ambiguous source fields after case normalization")
    problems = [
        name for name, kind in REQUIRED_FIELDS.items() if fields.get(name) != kind
    ]
    if problems:
        raise ValueError(
            f"Missing or incompatible source fields: {', '.join(problems)}"
        )
    geometry = layer.get("geometryFields", [])
    if len(geometry) != 1 or geometry[0].get("type") not in {"Polygon", "MultiPolygon"}:
        raise ValueError("Expected one two-dimensional polygon geometry field")
    crs_id = geometry[0].get("coordinateSystem", {}).get("projjson", {}).get("id", {})
    if crs_id.get("authority") != "EPSG" or str(crs_id.get("code")) != str(
        contract.srid
    ):
        raise ValueError(f"Expected source CRS EPSG:{contract.srid}")
    count = layer.get("featureCount")
    if type(count) is not int or count <= 0 or count != contract.expected_count:
        raise ValueError(
            f"Incomplete source: measured {count}, expected {contract.expected_count} for {contract.release}"
        )
    return count
