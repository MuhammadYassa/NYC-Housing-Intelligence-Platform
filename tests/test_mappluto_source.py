import hashlib
import subprocess
from pathlib import Path
from zipfile import ZipFile

import pytest
from conftest import inspection_for
from psycopg.conninfo import conninfo_to_dict
from pydantic import SecretStr

from housing_intelligence.config import Settings
from housing_intelligence.database import (
    connection_parameters,
    gdal_connection,
    sqlalchemy_url,
)
from housing_intelligence.pipeline.mappluto.bronze import (
    calculate_sha256,
    extract_mappluto,
    verify_bronze_artifact,
)
from housing_intelligence.pipeline.mappluto.gdal import _run_command, load_to_staging
from housing_intelligence.pipeline.mappluto.source import (
    REQUIRED_FIELDS,
    runtime_contract,
    contract_for_release,
    validate_inspection,
)


def test_checksum(archive):
    expected = hashlib.sha256(archive.read_bytes()).hexdigest()
    assert calculate_sha256(archive) == expected.upper()
    verify_bronze_artifact(archive, expected)
    with pytest.raises(ValueError, match="do not match"):
        verify_bronze_artifact(archive, "0" * 64)
    with pytest.raises(FileNotFoundError):
        verify_bronze_artifact(archive.parent / "missing.zip", expected)


@pytest.mark.parametrize(
    "members,exception",
    [
        (["readme.txt"], FileNotFoundError),
        (["one.gdb/a", "two.gdb/a"], ValueError),
        (["../escape", "one.gdb/a"], ValueError),
    ],
)
def test_invalid_archive(tmp_path, members, exception):
    archive = tmp_path / "source.zip"
    with ZipFile(archive, "w") as output:
        for member in members:
            output.writestr(member, "data")
    with pytest.raises(exception):
        extract_mappluto(archive, tmp_path / "run")
    assert not (tmp_path / "escape").exists()


def test_workspace_is_never_reused_or_deleted(archive, tmp_path):
    first = extract_mappluto(archive, tmp_path / "one")
    with pytest.raises(FileExistsError):
        extract_mappluto(archive, tmp_path / "one")
    second = extract_mappluto(archive, tmp_path / "two")
    assert first.is_dir() and second.is_dir()
    assert (first / "table").read_text() == "synthetic source"


def test_contract(contract):
    assert validate_inspection(inspection_for(contract), contract) == 1
    assert contract_for_release("26v1").expected_count == 856614
    with pytest.raises(ValueError, match="Unsupported"):
        contract_for_release("unknown")


@pytest.mark.parametrize(
    "defect", ["driver", "layer", "field", "type", "geometry", "crs", "count"]
)
def test_source_contract_rejects_incompatible_input(contract, defect):
    info = inspection_for(contract)
    layer = info["layers"][0]
    if defect == "driver":
        info["driverShortName"] = "GeoJSON"
    elif defect == "layer":
        layer["name"] = "wrong"
    elif defect == "field":
        layer["fields"].pop()
    elif defect == "type":
        layer["fields"][0]["type"] = "String"
    elif defect == "geometry":
        layer["geometryFields"][0]["type"] = "Point"
    elif defect == "crs":
        layer["geometryFields"][0]["coordinateSystem"]["projjson"]["id"]["code"] = 4326
    else:
        layer["featureCount"] = 0
    with pytest.raises(ValueError):
        validate_inspection(info, contract)


def test_one_database_configuration(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://wrong:wrong@wrong/wrong")
    settings = Settings(_env_file=None, database_password=SecretStr("a% @'\\secret"))
    params = connection_parameters(settings)
    url = sqlalchemy_url(settings)
    assert (url.host, url.port, url.database, url.username, url.password) == (
        params["host"],
        params["port"],
        params["dbname"],
        params["user"],
        params["password"],
    )
    gdal = conninfo_to_dict(gdal_connection(settings)[3:])
    assert gdal["dbname"] == params["dbname"]
    assert "password" not in gdal
    assert "secret" not in repr(settings)


def test_gdal_timeout(monkeypatch):
    def timeout(*args, **kwargs):
        assert kwargs["timeout"] == 3
        raise subprocess.TimeoutExpired(args[0], 3)

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(RuntimeError, match="exceeded 3"):
        _run_command(["ogrinfo"], timeout=3)


def test_gdal_error(monkeypatch):
    def failure(*args, **kwargs):
        raise subprocess.CalledProcessError(1, args[0], stderr="bad input")

    monkeypatch.setattr(subprocess, "run", failure)
    with pytest.raises(RuntimeError, match="bad input"):
        _run_command(["ogrinfo"], timeout=3)


def test_gdal_load_uses_contract_and_secret_environment(monkeypatch, contract):
    settings = Settings(_env_file=None, database_password=SecretStr("test-secret"))

    def capture(arguments, **kwargs):
        assert contract.layer in arguments
        assert "test-secret" not in str(arguments)
        assert kwargs["env"]["PGPASSWORD"] == "test-secret"
        assert kwargs["timeout"] == settings.gdal_load_timeout_seconds
        assert "-overwrite" in arguments and "-preserve_fid" in arguments
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(subprocess, "run", capture)
    load_to_staging(Path("source.gdb"), contract, settings)


@pytest.mark.parametrize("field", list(REQUIRED_FIELDS))
@pytest.mark.parametrize("defect", ["missing", "type"])
def test_runtime_requires_every_application_field(contract, field, defect):
    inspection = inspection_for(contract)
    fields = inspection["layers"][0]["fields"]
    if defect == "missing":
        inspection["layers"][0]["fields"] = [
            item for item in fields if item["name"] != field
        ]
    else:
        next(item for item in fields if item["name"] == field)["type"] = (
            "UnsupportedType"
        )
    with pytest.raises(ValueError, match="incompatible"):
        runtime_contract("25v2.1", inspection)


@pytest.mark.parametrize("count", [None, 0, -1, True, 1.5])
def test_runtime_rejects_invalid_feature_counts(contract, count):
    inspection = inspection_for(contract)
    inspection["layers"][0]["featureCount"] = count
    with pytest.raises(ValueError):
        runtime_contract("26v2", inspection)


@pytest.mark.parametrize("release", ["../26v2", "26v", "26v2.1.2", "26v2/", ""])
def test_runtime_rejects_invalid_release_identifiers(contract, release):
    with pytest.raises(ValueError):
        runtime_contract(release, inspection_for(contract))


def test_runtime_rejects_layer_and_field_ambiguity(contract):
    inspection = inspection_for(contract)
    inspection["layers"].append(dict(inspection["layers"][0]))
    with pytest.raises(ValueError, match="exactly one"):
        runtime_contract("26v2", inspection)
    inspection = inspection_for(contract)
    inspection["layers"][0]["fields"].append({"name": "BBL", "type": "Real"})
    with pytest.raises(ValueError, match="Ambiguous"):
        runtime_contract("26v2", inspection)


def test_gdal_zero_exit_does_not_hide_error_diagnostic(monkeypatch):
    from housing_intelligence.pipeline.mappluto.errors import OperationalError

    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], 0, "", "ERROR 1: guarded destination open failed"
        ),
    )
    with pytest.raises(OperationalError, match="guarded destination"):
        _run_command(["ogr2ogr"], timeout=3)


def test_real_subprocess_timeout_kills_and_reaps_child(monkeypatch):
    import sys
    from housing_intelligence.pipeline.mappluto.errors import OperationalError

    original = subprocess.Popen
    children = []

    def spawn(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(subprocess, "Popen", spawn)
    with pytest.raises(OperationalError, match="exceeded"):
        _run_command([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.1)
    assert len(children) == 1 and children[0].poll() is not None
