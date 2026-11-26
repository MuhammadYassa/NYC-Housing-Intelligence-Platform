"""Compare validated staging with counts of the last successful run."""

from ...config import Settings


class QualityError(ValueError):
    def __init__(self, summary: dict):
        self.summary = summary
        super().__init__(f"Cross-release quality gate rejected snapshot: {summary}")


def latest_success(conn):
    return conn.execute("""
        SELECT id, source_version, checksum_sha256, source_row_count, validation_summary
        FROM etl.import_runs WHERE dataset_name='MapPLUTO' AND status='COMPLETED'
        ORDER BY completed_at DESC, id DESC LIMIT 1
    """).fetchone()


def policy(settings: Settings) -> dict:
    values = {
        "max_total_change": settings.mappluto_max_total_change,
        "max_borough_change": settings.mappluto_max_borough_change,
    }
    if any(value is None for value in values.values()):
        raise ValueError(
            "Both MapPLUTO anomaly thresholds must be explicitly configured"
        )
    return values


def borough_counts(conn) -> dict:
    return {
        str(code): count
        for code, count in conn.execute(
            "SELECT borocode, count(*) FROM staging.mappluto_source GROUP BY borocode"
        ).fetchall()
    }


def check_snapshot(
    conn, count: int, settings: Settings, arcgis_count: int
) -> tuple[dict, dict]:
    thresholds = policy(settings)
    previous = latest_success(conn)
    current = borough_counts(conn)
    if set(current) != {"1", "2", "3", "4", "5"}:
        raise QualityError({"violations": ["All five boroughs must be present"]})
    if previous is None:
        return {
            "baseline_run_id": None,
            "source_count": count,
            "arcgis_count": arcgis_count,
            "arcgis_count_difference": count - arcgis_count,
            "violations": [],
        }, current
    baseline = previous[4]["snapshot"]
    old_count, old_boroughs = baseline["feature_count"], baseline["borough_counts"]
    violations = []
    if set(current) != {"1", "2", "3", "4", "5"} or set(old_boroughs) != set(current):
        violations.append("All five boroughs must be present in both snapshots")
    overall = abs(count - old_count) / old_count
    if overall > thresholds["max_total_change"]:
        violations.append("Overall count change exceeds threshold")
    borough_changes = {}
    for borough in sorted(set(current) | set(old_boroughs)):
        old = old_boroughs.get(borough, 0)
        new = current.get(borough, 0)
        change = abs(new - old) / old if old else None
        borough_changes[borough] = {"previous": old, "current": new, "change": change}
        if change is None or change > thresholds["max_borough_change"]:
            violations.append(f"Borough {borough} count change exceeds threshold")
    summary = {
        "baseline_run_id": previous[0],
        "previous_count": old_count,
        "source_count": count,
        "total_change": overall,
        "boroughs": borough_changes,
        "thresholds": thresholds,
        # This is a comparison signal, deliberately not a completeness requirement.
        "arcgis_count": arcgis_count,
        "arcgis_count_difference": count - arcgis_count,
        "violations": violations,
    }
    if violations:
        raise QualityError(summary)
    return summary, current
