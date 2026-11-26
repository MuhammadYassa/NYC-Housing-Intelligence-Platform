# MapPLUTO operations

## Normal runs

`python -m housing_intelligence.pipeline.mappluto.automation` discovers the official
shoreline-clipped FileGDB, downloads changed publications, and uses the same
staging/validation/merge path as manual imports. One advisory lock covers both.
No scheduler is installed by the command.

Set `MAPPLUTO_MAX_TOTAL_CHANGE` and `MAPPLUTO_MAX_BOROUGH_CHANGE` explicitly.
Both are allowed fractions of change from the last successful source. Review
appropriate limits before scheduling. Total and borough counts must pass; all
five boroughs must exist. Every existing row-level validation rule still applies.
ArcGIS count differences are diagnostics, not exact completeness requirements.
Equal counts can hide widespread BBL replacement; exact membership is not stored.

Unchanged metadata and previously imported release/checksum pairs are logged
without creating import runs. Changed checksums under the same release are
logged as republications and validated again. `--force-download` checks bytes
when metadata is unchanged; normal checks cannot detect replacements with
entirely unchanged publication metadata.

HTTP uses Requests with a 10-second connect timeout and the configured read
timeout (60 seconds by default), validated redirects, and three attempts for
transient failures. These timeouts do not bound total duration. Downloads are
streamed, with compressed/expanded limits of 2 GB/8 GB by default. Ingestion is
attempted once per invocation. Discovery and download failures are logged;
artifacts entering ingestion also have durable import-run diagnostics.

Successful bronze ZIPs are retained. Temporary downloads and extraction files
are cleaned where possible. Finalized failed archives are not automatically
pruned. After process death, inspect and remove leftover working files manually.

## Fresh database initialization

From the repository root, install the project in your virtual environment:

```powershell
.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

If `.env` does not exist, copy `.env.example` to `.env`. Set the password,
PostgreSQL connection settings, GDAL executable paths, and the two count-change
thresholds. No command here overwrites an existing `.env` or deletes volumes.
Use the fresh database volume you selected; the migration chain does not support
previous database schemas.

```powershell
docker compose up -d --wait postgres
.venv\Scripts\python.exe -m alembic upgrade head
.venv\Scripts\python.exe -m housing_intelligence.pipeline.mappluto.automation
```

The two migrations create PostGIS and the `etl`, `staging`, and `silver` schemas,
the complete `etl.import_runs` table, and `silver.tax_lots`
with its spatial constraints, lineage foreign key, and indexes. GDAL creates
and replaces `staging.mappluto_source` during ingestion. No candidate, check,
snapshot, or membership tables are created.

The first automated import performs every source and staging validation and
requires all five boroughs. There is no earlier release to compare, so it stores
its counts as the first baseline in the same transaction as merge/completion.
Later automated imports compare total and borough counts against the last
successful run. Failed imports cannot become baselines. Reviewed manual archive
imports retain their existing validation path and save counts for automation;
they are not a replacement for automated cross-release checks.

## Verification

```powershell
.venv\Scripts\python.exe -m pytest -q
.venv\Scripts\python.exe -m pytest -q --integration
.venv\Scripts\python.exe -m ruff check src tests alembic
.venv\Scripts\python.exe -m mypy src
```

Integration tests use a PostgreSQL/PostGIS server with `CREATE DATABASE` permission
and create, migrate, and destroy only randomly named disposable test databases.
The migration test upgrades an empty database, round-trips both revisions, and
checks the resulting schema and accounting constraints. Workflow tests cover
first-import validation, baseline creation, and subsequent count comparisons.

## Interrupted GDAL loads

The job commits `staging_recovery_required=true` immediately before launching
GDAL. Success, normal exit errors, failure to launch, and a timeout where Python
kills and reaps the subprocess clear it when no MapPLUTO database session remains.
A single session check prevents retrying over SQL left running by an exited client;
it does not cancel sessions or reacquire ownership. Unexpected interruption, parent death,
or inability to persist the known outcome can leave it set. An unresolved flag
blocks manual and automated imports.

To recover:

1. Pause scheduling and confirm the old Python process has stopped.
2. Inspect `ogr2ogr` processes and `pg_stat_activity` in the correct database.
   The application name is `mappluto-gdal`. Stop surviving writers and confirm their sessions
   have ended. A missing session alone does not prove a process cannot connect.
3. Start a database transaction and request
   `SELECT pg_try_advisory_xact_lock(741203, 1)`. If false, roll back and wait.
   Only after the preceding checks, clear `staging_recovery_required` for the
   inspected import-run IDs and commit. Do not rewrite completed import outcomes.
4. Rerun ingestion. It replaces staging and fully validates before promotion.

No automatic backend termination or ownership reconnection is performed. Database
failure stops the invocation; failure recording is best effort. The next owner
marks abandoned imports failed. If a commit acknowledgement is lost, inspect its
outcome before retrying: canonical changes and `COMPLETED` commit together, and
failure handling cannot overwrite a completed outcome.