"""Discover and ingest official MapPLUTO releases. Scheduling is external."""

import argparse
import json
import logging
from dataclasses import asdict
from pathlib import Path

from psycopg.types.json import Jsonb
from pydantic import ValidationError as SettingsValidationError

from ...config import Settings
from ...database import connect
from .acquisition import acquire
from .discovery import OfficialHTTP, discover, retry
from .job import mappluto_lock, recover_interrupted, run_mappluto
from .quality import latest_success, policy

LOGGER = logging.getLogger(__name__)


def error_message(error):
    if isinstance(error, SettingsValidationError):
        return "Invalid settings: " + ", ".join(
            f"{'.'.join(map(str, item['loc']))}: {item['type']}"
            for item in error.errors(include_input=False)
        )
    return f"{type(error).__name__}: {error}"[:8000]


def release_order(version):
    year, remainder = version.split("v")
    major, _, minor = remainder.partition(".")
    return int(year), int(major), int(minor or "0")


def skipped(reason):
    LOGGER.info("MapPLUTO unchanged: %s", reason)
    return {"status": "SKIPPED", "reason": reason}


def run_daily(
    settings: Settings,
    bronze_root: Path,
    work_root: Path,
    *,
    force_download: bool = False,
) -> dict:
    with connect(settings) as conn, mappluto_lock(conn):
        recover_interrupted(conn)
        policy(settings)
        source = retry(lambda: discover(OfficialHTTP(settings)))
        previous = latest_success(conn)
        if previous is not None and release_order(source.version) < release_order(
            previous[1]
        ):
            raise ValueError(
                "Discovered release is older than the last successful import"
            )
        metadata = source.asdict()
        if (
            not force_download
            and conn.execute(
                """SELECT 1 FROM etl.import_runs WHERE dataset_name='MapPLUTO'
               AND status='COMPLETED' AND source_version=%s AND source_metadata=%s""",
                (source.version, Jsonb(metadata)),
            ).fetchone()
        ):
            return skipped("official publication metadata matches a successful import")

        archive, checksum, _ = retry(
            lambda: acquire(OfficialHTTP(settings), source, bronze_root)
        )
        imported = conn.execute(
            """SELECT id FROM etl.import_runs WHERE dataset_name='MapPLUTO'
               AND status='COMPLETED' AND source_version=%s
               AND upper(checksum_sha256)=%s ORDER BY completed_at DESC, id DESC LIMIT 1""",
            (source.version, checksum),
        ).fetchone()
        if imported:
            # Refresh headers so identical bytes do not cause repeated downloads.
            conn.execute(
                "UPDATE etl.import_runs SET source_metadata=%s WHERE id=%s",
                (Jsonb(metadata), imported[0]),
            )
            return skipped(f"artifact already imported by run {imported[0]}")
        if conn.execute(
            """SELECT 1 FROM etl.import_runs WHERE dataset_name='MapPLUTO'
               AND status='COMPLETED' AND source_version=%s""",
            (source.version,),
        ).fetchone():
            LOGGER.warning(
                "MapPLUTO %s republished with SHA-256 %s", source.version, checksum
            )
        result = run_mappluto(
            archive,
            source.version,
            checksum,
            settings=settings,
            work_root=work_root,
            connection=conn,
            source_metadata=metadata,
        )
        return {"status": "COMPLETED", **asdict(result)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze-root", type=Path, default=Path("data/bronze/mappluto")
    )
    parser.add_argument(
        "--work-root", type=Path, default=Path("data/working/mappluto-automation")
    )
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        settings = Settings()
        result = run_daily(
            settings,
            args.bronze_root,
            args.work_root,
            force_download=args.force_download,
        )
        print(json.dumps(result))
        return 0
    except Exception as error:
        LOGGER.error("MapPLUTO automation failed: %s", error_message(error))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
