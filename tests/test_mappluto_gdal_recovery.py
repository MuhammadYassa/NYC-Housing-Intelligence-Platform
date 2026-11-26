"""Real FileGDBs, staging interruption flags, and parent-process termination."""

import json
import shutil
from zipfile import ZipFile

import pytest

from housing_intelligence.database import connect
from housing_intelligence.pipeline.mappluto import automation as a, job
from housing_intelligence.pipeline.mappluto.bronze import calculate_sha256
from housing_intelligence.pipeline.mappluto.gdal import (
    _run_command,
    gdal_environment,
)
from housing_intelligence.pipeline.mappluto.source import SourceContract
from test_mappluto_automation_database import configured as configured, rows

pytestmark = pytest.mark.integration


def filegdb(root, selected_rows, settings, name):
    if not shutil.which(settings.ogr2ogr_executable) or not shutil.which(
        settings.ogrinfo_executable
    ):
        pytest.skip("GDAL unavailable: real FileGDB/lifecycle verification did not run")
    root.mkdir(parents=True, exist_ok=True)
    source = root / (name + ".geojson")
    features = []
    for row in selected_rows:
        props = {
            key: value for key, value in row.items() if key not in {"shape", "srid"}
        }
        features.append(
            {
                "type": "Feature",
                "properties": props,
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [1000000, 200000],
                            [1000010, 200000],
                            [1000010, 200010],
                            [1000000, 200010],
                            [1000000, 200000],
                        ]
                    ],
                },
            }
        )
    source.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}),
        encoding="utf-8",
    )
    gdb = root / (name + ".gdb")
    _run_command(
        [
            settings.ogr2ogr_executable,
            "-f",
            "OpenFileGDB",
            str(gdb),
            str(source),
            "-nln",
            "fixture",
            "-a_srs",
            "EPSG:2263",
            "-nlt",
            "MULTIPOLYGON",
        ],
        timeout=30,
        env=gdal_environment(settings),
    )
    archive = root / (name + ".zip")
    with ZipFile(archive, "w") as output:
        for path in gdb.rglob("*"):
            if path.is_file():
                output.write(path, path.relative_to(root))
    return gdb, archive


def test_full_workflow_real_gdal_and_same_version_correction(
    db, configured, tmp_path, monkeypatch
):
    _, base = filegdb(tmp_path, rows(1), configured, "base")
    contract = SourceContract("26v1", "fixture", 2026, 5)
    job.run_mappluto(
        base,
        contract,
        calculate_sha256(base),
        settings=configured,
        work_root=tmp_path / "manual",
    )
    from housing_intelligence.pipeline.mappluto.discovery import Discovery

    state = {"owner": "NEW", "revision": 1}

    def prepare():
        _, archive = filegdb(
            tmp_path,
            rows(1, "26v2", state["owner"]),
            configured,
            f"release-{state['revision']}",
        )
        source = Discovery(
            "26v2",
            state["revision"],
            12,
            "official",
            "official",
            archive.stat().st_size,
            str(state["revision"]),
            None,
        )
        monkeypatch.setattr(a, "discover", lambda _: source)
        monkeypatch.setattr(
            a,
            "acquire",
            lambda *args: (archive, calculate_sha256(archive), archive.stat().st_size),
        )

    prepare()
    assert (
        a.run_daily(configured, tmp_path / "bronze", tmp_path / "working")["status"]
        == "COMPLETED"
    )
    assert (
        a.run_daily(configured, tmp_path / "bronze", tmp_path / "working")["status"]
        == "SKIPPED"
    )
    state.update(owner="CORRECTED", revision=2)
    prepare()
    assert (
        a.run_daily(configured, tmp_path / "bronze", tmp_path / "working")["status"]
        == "COMPLETED"
    )
    assert db.execute("SELECT DISTINCT owner_name FROM silver.tax_lots").fetchall() == [
        ("CORRECTED",)
    ]


def test_real_gdal_timeout_with_surviving_backend_requires_clearance(
    db, configured, tmp_path
):
    from housing_intelligence.pipeline.import_runs import create_import_run

    gdb, archive = filegdb(tmp_path, rows(1), configured, "timeout")
    settings = configured.model_copy(update={"gdal_load_timeout_seconds": 1})
    db.execute("CREATE TABLE staging.mappluto_source (marker integer)")
    with connect(settings) as blocker, job.mappluto_lock(db):
        blocker.execute("BEGIN")
        blocker.execute("LOCK TABLE staging.mappluto_source IN ACCESS EXCLUSIVE MODE")
        try:
            run_id = create_import_run(
                db, "MapPLUTO", job.PIPELINE_VERSION, "26v1", archive, None
            )
            with pytest.raises(RuntimeError, match="database session remains"):
                job.load_staging_for_run(
                    db,
                    run_id,
                    gdb,
                    SourceContract("26v1", "fixture", 2026, 5),
                    settings,
                )
            assert db.execute(
                "SELECT staging_recovery_required FROM etl.import_runs WHERE id=%s",
                (run_id,),
            ).fetchone() == (True,)
        finally:
            blocker.execute("ROLLBACK")


def test_killed_parent_leaves_durable_staging_flag(db, configured, tmp_path):
    import os
    import subprocess
    import sys
    import time
    from pathlib import Path
    from housing_intelligence.pipeline.import_runs import create_import_run

    run_id = create_import_run(
        db, "MapPLUTO", job.PIPELINE_VERSION, "26v1", Path("source.zip"), None
    )
    script = tmp_path / "interrupted_owner.py"
    script.write_text(
        "import sys, time\n"
        "from housing_intelligence.config import Settings\n"
        "from housing_intelligence.database import connect\n"
        "from housing_intelligence.pipeline.mappluto import job\n"
        "s = Settings(_env_file=None)\n"
        "job.load_to_staging = lambda *args: time.sleep(60)\n"
        "with connect(s) as conn, job.mappluto_lock(conn):\n"
        "    job.load_staging_for_run(conn, int(sys.argv[1]), None, None, s)\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        POSTGRES_HOST=configured.database_host,
        POSTGRES_HOST_PORT=str(configured.database_port),
        POSTGRES_DB=configured.database_name,
        POSTGRES_USER=configured.database_user,
        POSTGRES_PASSWORD=configured.database_password.get_secret_value(),
    )
    parent = subprocess.Popen(
        [sys.executable, str(script), str(run_id)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            flagged = db.execute(
                "SELECT staging_recovery_required FROM etl.import_runs WHERE id=%s",
                (run_id,),
            ).fetchone()[0]
            if flagged:
                break
            assert parent.poll() is None, "Test owner exited before starting load"
            time.sleep(0.03)
        assert flagged
        parent.kill()
        parent.wait(timeout=5)
        # PostgreSQL observes socket closure asynchronously after process death.
        deadline = time.monotonic() + 5
        while not db.execute("SELECT pg_try_advisory_lock(741203,1)").fetchone()[0]:
            assert time.monotonic() < deadline, "Killed owner's lock was not released"
            time.sleep(0.03)
        db.execute("SELECT pg_advisory_unlock(741203,1)")
        with job.mappluto_lock(db), pytest.raises(RuntimeError, match="Uncertain GDAL"):
            job.recover_interrupted(db)
        assert db.execute(
            "SELECT status,staging_recovery_required FROM etl.import_runs WHERE id=%s",
            (run_id,),
        ).fetchone() == ("FAILED", True)
    finally:
        if parent.poll() is None:
            parent.kill()
        parent.wait(timeout=5)
