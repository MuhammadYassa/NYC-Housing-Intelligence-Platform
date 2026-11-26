"""One MapPLUTO run owns staging and commits canonical changes with its outcome."""

import argparse
import json
import logging
import tempfile
import shutil
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path

import psycopg
from psycopg.pq import TransactionStatus

from ...config import Settings
from ...database import connect
from ...models.status import ImportRunStatus
from ..import_runs import (
    complete_import_run,
    create_import_run,
    fail_import_run,
    record_import_progress,
    update_import_status,
)
from .bronze import calculate_sha256, extract_mappluto
from .gdal import inspect_mappluto, load_to_staging
from .errors import OperationalError
from .quality import QualityError, borough_counts, check_snapshot
from .source import (
    SourceContract,
    contract_for_release,
    validate_inspection,
    runtime_contract,
)
from .transform import merge_tax_lots
from .validation import ValidationError, validate_staging

LOGGER = logging.getLogger(__name__)
PIPELINE_VERSION = "mappluto-v1"
# Fixed two-int namespace/key, independent of Python's randomized hash().
MAPPLUTO_LOCK = (741203, 1)


@contextmanager
def mappluto_lock(conn):
    if not conn.execute(
        "SELECT pg_try_advisory_lock(%s, %s)", MAPPLUTO_LOCK
    ).fetchone()[0]:
        raise RuntimeError(
            "Another MapPLUTO job owns staging; retry after it finishes."
        )
    try:
        yield
    finally:
        # Closing a broken connection also releases this session-level lock.
        if not conn.closed:
            try:
                conn.execute("SELECT pg_advisory_unlock(%s, %s)", MAPPLUTO_LOCK)
            except psycopg.Error:
                conn.close()
                LOGGER.warning(
                    "Lock connection lost; PostgreSQL releases ownership when the session ends"
                )


@dataclass(frozen=True)
class RunResult:
    run_id: int
    inserted: int
    updated: int
    unchanged: int


class MapplutoRunError(RuntimeError):
    def __init__(self, run_id: int, message: str):
        self.run_id = run_id
        super().__init__(f"MapPLUTO run {run_id} failed: {message}")


def recover_interrupted(conn):
    """Caller must own the MapPLUTO advisory lock."""
    conn.execute(
        """UPDATE etl.import_runs SET status='FAILED', completed_at=clock_timestamp(),
           error_message='Interrupted run recovered by the next MapPLUTO owner',
           inserted_count=0, updated_count=0, unchanged_count=0
           WHERE dataset_name='MapPLUTO'
             AND status NOT IN ('COMPLETED','FAILED')""",
    )

    pending = conn.execute(
        "SELECT id FROM etl.import_runs WHERE dataset_name='MapPLUTO' "
        "AND staging_recovery_required ORDER BY id"
    ).fetchall()
    if pending:
        raise RuntimeError(
            f"Uncertain GDAL termination for runs {[r[0] for r in pending]}; "
            "confirm processes and database sessions have stopped before clearing the flag"
        )


def load_staging_for_run(conn, run_id, gdb, contract, settings):
    """Persist uncertainty before spawning; only known subprocess outcomes clear it."""
    conn.execute(
        "UPDATE etl.import_runs SET staging_recovery_required=true WHERE id=%s",
        (run_id,),
    )
    error = None
    try:
        load_to_staging(gdb, contract, settings)
    except OperationalError as load_error:
        # _run_command returns this only after subprocess exit, failure to start,
        # or a timeout whose child subprocess.run has killed and reaped.
        error = load_error
    # A dead client can leave an SQL command running on the server. Do not let
    # another staging load proceed while a known MapPLUTO session still exists.
    active = conn.execute(
        """SELECT EXISTS (SELECT 1 FROM pg_stat_activity
           WHERE datname=current_database() AND usename=current_user
           AND application_name='mappluto-gdal' AND pid<>pg_backend_pid())"""
    ).fetchone()[0]
    if active:
        raise RuntimeError(
            "GDAL exited but its database session remains; inspect it before clearing the flag"
        ) from error
    conn.execute(
        "UPDATE etl.import_runs SET staging_recovery_required=false WHERE id=%s",
        (run_id,),
    )
    if error is not None:
        raise error


def run_mappluto(
    archive: Path,
    contract: SourceContract | str,
    expected_sha256: str,
    *,
    settings: Settings | None = None,
    work_root: Path = Path("data/working/mappluto"),
    connection: psycopg.Connection | None = None,
    source_metadata: dict | None = None,
) -> RunResult:
    settings = settings or Settings()
    archive = archive.resolve()
    if connection is not None and (
        not connection.autocommit
        or connection.info.transaction_status != TransactionStatus.IDLE
    ):
        raise ValueError("Injected MapPLUTO connection must be idle and autocommit")
    if connection is not None and connection.info.dbname != settings.database_name:
        raise ValueError(
            "Injected MapPLUTO connection must use the configured database"
        )
    # Automation can keep discovery ownership and staging ownership on the
    # same session. Manual imports continue to create/own their connection.
    with (
        (
            nullcontext(connection) if connection is not None else connect(settings)
        ) as conn,
        mappluto_lock(conn),
    ):
        recover_interrupted(conn)
        with conn.transaction():
            run_id = create_import_run(
                conn,
                "MapPLUTO",
                PIPELINE_VERSION,
                contract if isinstance(contract, str) else contract.release,
                archive,
                None,
            )
            if source_metadata is not None:
                record_import_progress(conn, run_id, source_metadata=source_metadata)
        phase = "artifact"
        validation = None
        workspace = None
        try:
            LOGGER.info("MapPLUTO run %s started", run_id)
            checksum = calculate_sha256(archive)
            record_import_progress(conn, run_id, checksum_sha256=checksum)
            if checksum != expected_sha256.upper():
                raise ValueError("SHA256 Hash do not match!")
            work_root = work_root.resolve()
            work_root.mkdir(parents=True, exist_ok=True)
            workspace = Path(tempfile.mkdtemp(prefix=f"run-{run_id}-", dir=work_root))
            LOGGER.info("Run workspace: %s", workspace)
            gdb = extract_mappluto(archive, workspace / "extracted")
            update_import_status(conn, run_id, ImportRunStatus.EXTRACTED)

            phase = "inspection"
            inspection = inspect_mappluto(
                gdb, None if isinstance(contract, str) else contract, settings
            )
            if isinstance(contract, str):
                contract = runtime_contract(contract, inspection)
            inspected_layers = [
                layer
                for layer in inspection.get("layers", [])
                if layer.get("name") == contract.layer
            ]
            if len(inspected_layers) == 1:
                measured = inspected_layers[0].get("featureCount")
                if type(measured) is int and measured >= 0:
                    record_import_progress(conn, run_id, source_row_count=measured)
            source_count = validate_inspection(inspection, contract)

            phase = "staging"
            load_staging_for_run(conn, run_id, gdb, contract, settings)
            staged_row = conn.execute(
                "SELECT count(*) FROM staging.mappluto_source"
            ).fetchone()
            if staged_row is None:
                raise RuntimeError("Could not measure staging row count")
            staged_count = staged_row[0]
            record_import_progress(conn, run_id, staged_row_count=staged_count)
            update_import_status(conn, run_id, ImportRunStatus.STAGED)

            phase = "validation"
            # Hold staging stable through validation and merge. The session lock
            # spans GDAL's separate connection; this table lock also blocks SQL
            # writers that do not cooperate with the job's advisory lock.
            with conn.transaction():
                conn.execute("LOCK TABLE staging.mappluto_source IN SHARE MODE")
                validation = validate_staging(conn, contract, source_count)
                if validation.rejected or validation.summary.get("completeness"):
                    raise ValidationError(validation)
                boroughs = borough_counts(conn)
                validation.summary["snapshot"] = {
                    "feature_count": validation.valid,
                    "borough_counts": boroughs,
                }
                if source_metadata is not None:
                    phase = "quality gate"
                    try:
                        summary, _ = check_snapshot(
                            conn,
                            validation.valid,
                            settings,
                            source_metadata["arcgis_count"],
                        )
                    except QualityError as error:
                        validation.summary["quality"] = error.summary
                        raise
                    validation.summary["quality"] = summary
                record_import_progress(
                    conn,
                    run_id,
                    valid_count=validation.valid,
                    rejected_count=validation.rejected,
                    validation_summary=validation.summary,
                )
                phase = "merge"
                counts = merge_tax_lots(conn, contract, run_id)
                if sum(asdict(counts).values()) != validation.valid:
                    raise RuntimeError(
                        "Merge counts do not reconcile with validated rows"
                    )
                complete_import_run(
                    conn, run_id, counts.inserted, counts.updated, 0, counts.unchanged
                )
            # Completion and canonical changes become visible together at commit.
            LOGGER.info("MapPLUTO run %s completed: %s", run_id, counts)
            return RunResult(run_id, counts.inserted, counts.updated, counts.unchanged)
        except (Exception, KeyboardInterrupt) as error:
            # transaction() rolled back first. Store diagnostics independently.
            message = f"{phase}: {type(error).__name__}: {error}"
            try:
                if isinstance(error, ValidationError):
                    validation = error.result
                with conn.transaction():
                    if validation is not None:
                        record_import_progress(
                            conn,
                            run_id,
                            staged_row_count=validation.received,
                            valid_count=validation.valid,
                            rejected_count=validation.rejected,
                            validation_summary=validation.summary,
                        )
                    fail_import_run(conn, run_id, message)
            except Exception:
                LOGGER.exception(
                    "Could not persist outcome for run %s; inspect/reconcile it before retry",
                    run_id,
                )
            raise MapplutoRunError(run_id, message) from error
        finally:
            if workspace is not None:
                try:
                    shutil.rmtree(workspace)
                except OSError:
                    LOGGER.warning(
                        "Could not clean workspace %s", workspace, exc_info=True
                    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument(
        "--sha256",
        required=True,
        help="Expected SHA-256 of the selected source archive",
    )
    parser.add_argument("--work-root", type=Path, default=Path("data/working/mappluto"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        result = run_mappluto(
            args.archive,
            contract_for_release(args.release),
            args.sha256,
            work_root=args.work_root,
        )
    except Exception as error:
        LOGGER.error("%s", error)
        return 1
    print(json.dumps(asdict(result)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
