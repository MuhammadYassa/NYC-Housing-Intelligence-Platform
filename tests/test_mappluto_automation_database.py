"""Real PostgreSQL/PostGIS tests for the simplified workflow."""

from pathlib import Path
from unittest.mock import Mock
from zipfile import ZipFile

import pytest

from conftest import lot
from housing_intelligence.pipeline.import_runs import create_import_run
from housing_intelligence.pipeline.mappluto import automation as a, job
from housing_intelligence.pipeline.mappluto.bronze import calculate_sha256
from housing_intelligence.pipeline.mappluto.discovery import Discovery, TransientError
from housing_intelligence.pipeline.mappluto.quality import check_snapshot
from housing_intelligence.pipeline.mappluto.source import SourceContract

pytestmark = pytest.mark.integration
URL = "https://s-media.nyc.gov/agencies/dcp/source.zip"


def rows(per_borough=4, version="26v1", owner="BASE"):
    return [
        lot(
            bbl=float(b * 1000000000 + 10000 + i),
            borocode=b,
            borough=code,
            lot=i,
            version=version,
            ownername=owner,
        )
        for b, code in enumerate(("MN", "BX", "BK", "QN", "SI"), 1)
        for i in range(1, per_borough + 1)
    ]


@pytest.fixture
def configured(db_settings, monkeypatch):
    monkeypatch.setattr(
        "housing_intelligence.pipeline.mappluto.discovery.time.sleep", lambda _: None
    )
    return db_settings.model_copy(
        update={
            "mappluto_max_total_change": 0.1,
            "mappluto_max_borough_change": 0.1,
        }
    )


@pytest.fixture
def baseline(db, configured, job_source, archive, tmp_path):
    selected = job_source(rows(), SourceContract("26v1", "fixture", 2026, 20))
    return job.run_mappluto(
        archive,
        selected,
        calculate_sha256(archive),
        settings=configured,
        work_root=tmp_path / "manual",
    )


def prepare(
    configured, job_source, monkeypatch, tmp_path, selected_rows=None, marker="new"
):
    selected_rows = (
        selected_rows
        if selected_rows is not None
        else rows(version="26v2", owner="NEW")
    )
    version = selected_rows[0]["version"]
    job_source(
        selected_rows, SourceContract(version, "fixture", 2026, len(selected_rows))
    )
    archive = tmp_path / (marker + ".zip")
    with ZipFile(archive, "w") as output:
        output.writestr("source.gdb/table", marker)
    source = Discovery(
        version,
        1,
        len(selected_rows) + 7,
        URL,
        URL,
        archive.stat().st_size,
        marker,
        None,
    )
    monkeypatch.setattr(a, "discover", lambda _: source)
    acquisition = Mock(
        return_value=(archive, calculate_sha256(archive), archive.stat().st_size)
    )
    monkeypatch.setattr(a, "acquire", acquisition)
    return source, archive, acquisition


def daily(configured, tmp_path, **kwargs):
    return a.run_daily(configured, tmp_path / "bronze", tmp_path / "working", **kwargs)


def test_success_metadata_skip_checksum_skip_and_republication(
    db, configured, baseline, job_source, monkeypatch, tmp_path, caplog
):
    monkeypatch.setattr(a.LOGGER, "disabled", False)
    source, archive, acquisition = prepare(
        configured, job_source, monkeypatch, tmp_path
    )
    result = daily(configured, tmp_path)
    assert result["status"] == "COMPLETED" and result["updated"] == 20
    before = db.execute("SELECT * FROM silver.tax_lots ORDER BY bbl").fetchall()
    assert daily(configured, tmp_path)["status"] == "SKIPPED"
    assert acquisition.call_count == 1
    assert daily(configured, tmp_path, force_download=True)["status"] == "SKIPPED"
    assert acquisition.call_count == 2
    assert db.execute("SELECT count(*) FROM etl.import_runs").fetchone()[0] == 2
    assert before == db.execute("SELECT * FROM silver.tax_lots ORDER BY bbl").fetchall()
    assert archive.exists()
    assert not list((tmp_path / "working").iterdir())

    prepare(
        configured,
        job_source,
        monkeypatch,
        tmp_path,
        rows(version="26v2", owner="CORRECTED"),
        marker="corrected",
    )
    assert daily(configured, tmp_path)["status"] == "COMPLETED"
    assert "republished" in caplog.text
    assert db.execute("SELECT DISTINCT owner_name FROM silver.tax_lots").fetchall() == [
        ("CORRECTED",)
    ]
    assert (
        db.execute(
            "SELECT count(*) FROM etl.import_runs WHERE status='COMPLETED'"
        ).fetchone()[0]
        == 3
    )


@pytest.mark.parametrize(
    "defect", ["identity", "geometry", "partial", "total", "borough", "version"]
)
def test_rejected_release_preserves_canonical_and_counts(
    db, configured, baseline, job_source, monkeypatch, tmp_path, defect
):
    selected = rows(version="26v2")
    if defect == "identity":
        selected[0]["bbl"] += 0.5
    elif defect == "geometry":
        selected[0]["shape"] = "POINT(0 0)"
    elif defect == "total":
        selected = rows(5, version="26v2")
    elif defect == "borough":
        selected = selected[:-1]
    elif defect == "version":
        selected[-1]["version"] = "25v1"
    prepare(configured, job_source, monkeypatch, tmp_path, selected)
    if defect == "partial":
        from conftest import inspection_for

        contract = SourceContract("26v2", "fixture", 2026, 21)
        monkeypatch.setattr(
            job, "inspect_mappluto", lambda *a: inspection_for(contract)
        )
    before = db.execute("SELECT * FROM silver.tax_lots ORDER BY bbl").fetchall()
    with pytest.raises(job.MapplutoRunError):
        daily(configured, tmp_path)
    assert before == db.execute("SELECT * FROM silver.tax_lots ORDER BY bbl").fetchall()
    assert db.execute(
        "SELECT status,staging_recovery_required FROM etl.import_runs ORDER BY id DESC LIMIT 1"
    ).fetchone() == ("FAILED", False)


def test_promotion_failure_rolls_back_merge_and_baseline(
    db, configured, baseline, job_source, monkeypatch, tmp_path
):
    prepare(configured, job_source, monkeypatch, tmp_path)
    before = db.execute("SELECT * FROM silver.tax_lots ORDER BY bbl").fetchall()
    complete = job.complete_import_run

    def fail(conn, *args):
        complete(conn, *args)
        raise RuntimeError("after merge before commit")

    monkeypatch.setattr(job, "complete_import_run", fail)
    with pytest.raises(job.MapplutoRunError):
        daily(configured, tmp_path)
    assert before == db.execute("SELECT * FROM silver.tax_lots ORDER BY bbl").fetchall()
    assert db.execute(
        "SELECT id FROM etl.import_runs WHERE status='COMPLETED'"
    ).fetchall() == [(baseline.run_id,)]


@pytest.mark.parametrize("kind", ["exit", "timeout", "missing", "uncertain"])
def test_gdal_failure_flag_and_retry(
    db, configured, baseline, job_source, monkeypatch, tmp_path, kind
):
    import subprocess
    from housing_intelligence.pipeline.mappluto import gdal

    prepare(configured, job_source, monkeypatch, tmp_path)
    if kind == "exit":
        error = subprocess.CalledProcessError(1, ["ogr2ogr"], stderr="bad input")
    elif kind == "timeout":
        error = subprocess.TimeoutExpired(["ogr2ogr"], 1)
    elif kind == "missing":
        error = FileNotFoundError("ogr2ogr")
    else:
        error = KeyboardInterrupt()
    monkeypatch.setattr(job, "load_to_staging", gdal.load_to_staging)
    monkeypatch.setattr(subprocess, "run", Mock(side_effect=error))
    with pytest.raises(job.MapplutoRunError):
        daily(configured, tmp_path)
    assert db.execute(
        "SELECT staging_recovery_required FROM etl.import_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()[0] == (kind == "uncertain")
    if kind == "uncertain":
        with pytest.raises(RuntimeError, match="Uncertain GDAL"):
            daily(configured, tmp_path)
        with pytest.raises(RuntimeError, match="Uncertain GDAL"):
            job.run_mappluto(
                tmp_path / "new.zip",
                SourceContract("26v2", "fixture", 2026, 20),
                "A" * 64,
                settings=configured,
            )
        # This explicit clearance is only in a disposable test database.
        db.execute("UPDATE etl.import_runs SET staging_recovery_required=false")
    prepare(configured, job_source, monkeypatch, tmp_path)
    assert daily(configured, tmp_path)["status"] == "COMPLETED"


def test_abandoned_run_marked_failed_without_touching_success(
    db, configured, baseline, tmp_path
):
    abandoned = create_import_run(
        db, "MapPLUTO", job.PIPELINE_VERSION, "26v2", Path("missing.zip"), None
    )
    db.execute(
        "UPDATE etl.import_runs SET staging_recovery_required=true WHERE id=%s",
        (abandoned,),
    )
    with pytest.raises(RuntimeError, match="Uncertain GDAL"):
        daily(configured, tmp_path)
    assert db.execute("SELECT status FROM etl.import_runs ORDER BY id").fetchall() == [
        ("COMPLETED",),
        ("FAILED",),
    ]


def test_quality_threshold_boundary_and_retained_absent_lots(db, configured, baseline):
    from conftest import stage

    stage(db, rows(4, version="26v2")[:-1])
    allowed = configured.model_copy(
        update={"mappluto_max_total_change": 0.05, "mappluto_max_borough_change": 0.25}
    )
    summary, _ = check_snapshot(db, 19, allowed, 999)
    assert (
        summary["total_change"] == 0.05 and summary["arcgis_count_difference"] == -980
    )
    assert db.execute("SELECT count(*) FROM silver.tax_lots").fetchone()[0] == 20


def test_old_release_blocks_download(
    db, configured, baseline, monkeypatch, tmp_path, job_source
):
    _, _, acquisition = prepare(
        configured, job_source, monkeypatch, tmp_path, rows(version="25v2.1")
    )
    with pytest.raises(ValueError, match="older"):
        daily(configured, tmp_path)
    acquisition.assert_not_called()


def test_download_retries_only_transient_failures(
    db, configured, baseline, monkeypatch, tmp_path, job_source
):
    _, archive, acquisition = prepare(configured, job_source, monkeypatch, tmp_path)
    acquisition.side_effect = [
        TransientError("offline"),
        TransientError("offline"),
        (archive, calculate_sha256(archive), archive.stat().st_size),
    ]
    assert daily(configured, tmp_path)["status"] == "COMPLETED"
    assert acquisition.call_count == 3


def test_manual_and_automation_share_lock(db, configured, baseline, tmp_path):
    with job.mappluto_lock(db), pytest.raises(RuntimeError, match="Another MapPLUTO"):
        daily(configured, tmp_path)


def test_cleanup_failure_cannot_reverse_completion(
    db, configured, baseline, monkeypatch, tmp_path, job_source
):
    prepare(configured, job_source, monkeypatch, tmp_path)
    monkeypatch.setattr(job.shutil, "rmtree", Mock(side_effect=OSError("busy")))
    assert daily(configured, tmp_path)["status"] == "COMPLETED"
    assert (
        db.execute(
            "SELECT status FROM etl.import_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        == "COMPLETED"
    )


def test_first_automated_import_establishes_baseline_then_compares(
    db, configured, job_source, monkeypatch, tmp_path
):
    prepare(configured, job_source, monkeypatch, tmp_path, rows(version="26v2"))
    first = daily(configured, tmp_path)
    assert first["inserted"] == 20
    first_summary = db.execute(
        "SELECT validation_summary FROM etl.import_runs WHERE id=%s", (first["run_id"],)
    ).fetchone()[0]
    assert first_summary["snapshot"] == {
        "feature_count": 20,
        "borough_counts": {str(b): 4 for b in range(1, 6)},
    }
    assert first_summary["quality"]["baseline_run_id"] is None
    prepare(
        configured,
        job_source,
        monkeypatch,
        tmp_path,
        rows(version="26v3", owner="NEXT"),
        marker="next",
    )
    second = daily(configured, tmp_path)
    summary = db.execute(
        "SELECT validation_summary FROM etl.import_runs WHERE id=%s",
        (second["run_id"],),
    ).fetchone()[0]
    assert summary["quality"]["baseline_run_id"] == first["run_id"]
    assert summary["quality"]["total_change"] == 0
    assert second["updated"] == 20
    prepare(
        configured,
        job_source,
        monkeypatch,
        tmp_path,
        rows(5, version="26v4"),
        marker="anomaly",
    )
    with pytest.raises(job.MapplutoRunError, match="count change exceeds"):
        daily(configured, tmp_path)
    assert db.execute("SELECT count(*) FROM silver.tax_lots").fetchone() == (20,)
    assert db.execute(
        "SELECT id FROM etl.import_runs WHERE status='COMPLETED' ORDER BY id"
    ).fetchall() == [
        (first["run_id"],),
        (second["run_id"],),
    ]


@pytest.mark.parametrize(
    "defect", ["identity", "geometry", "schema", "partial", "missing_borough"]
)
def test_first_import_validation_failure_cannot_establish_baseline(
    db, configured, job_source, monkeypatch, tmp_path, defect
):
    from conftest import inspection_for

    selected = rows(version="26v2")
    if defect == "identity":
        selected[0]["bbl"] += 0.5
    elif defect == "geometry":
        selected[0]["shape"] = "POINT(0 0)"
    elif defect == "missing_borough":
        selected = selected[:-4]
    prepare(configured, job_source, monkeypatch, tmp_path, selected)
    if defect in {"schema", "partial"}:
        inspection = inspection_for(
            SourceContract("26v2", "fixture", 2026, 21 if defect == "partial" else 20)
        )
        if defect == "schema":
            inspection["layers"][0]["fields"] = []
        monkeypatch.setattr(job, "inspect_mappluto", lambda *args: inspection)
    with pytest.raises(job.MapplutoRunError):
        daily(configured, tmp_path)
    assert db.execute("SELECT count(*) FROM silver.tax_lots").fetchone() == (0,)
    assert db.execute(
        "SELECT count(*) FROM etl.import_runs WHERE status='COMPLETED'"
    ).fetchone() == (0,)
    prepare(
        configured,
        job_source,
        monkeypatch,
        tmp_path,
        rows(version="26v2"),
        marker="valid",
    )
    first = daily(configured, tmp_path)
    assert db.execute(
        "SELECT validation_summary->'quality'->'baseline_run_id' FROM etl.import_runs WHERE id=%s",
        (first["run_id"],),
    ).fetchone() == (None,)
