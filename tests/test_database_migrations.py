"""Create the current schema from empty and round-trip the migration chain."""

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine

from conftest import ROOT
from housing_intelligence.database import sqlalchemy_url

pytestmark = pytest.mark.integration


def test_empty_database_migration_chain(db, db_settings):
    # db_settings initially upgrades a brand-new randomly named database.
    # This test also exercises downgrade/upgrade against that disposable DB.
    engine = create_engine(sqlalchemy_url(db_settings))
    try:
        with engine.begin() as conn:
            config = Config(str(ROOT / "alembic.ini"))
            config.attributes["connection"] = conn
            command.downgrade(config, "base")
            assert not conn.exec_driver_sql(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN ('etl','staging','bronze','silver','gold')"
            ).fetchall()
            command.upgrade(config, "head")
    finally:
        engine.dispose()
    assert db.execute("SELECT version_num FROM alembic_version").fetchone() == (
        "eb3717d79960",
    )
    assert db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='etl'"
    ).fetchall() == [("import_runs",)]
    assert db.execute("SELECT count(*) FROM etl.import_runs").fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM silver.tax_lots").fetchone() == (0,)
    columns = dict(
        db.execute(
            "SELECT column_name,udt_name FROM information_schema.columns "
            "WHERE table_schema='etl' AND table_name='import_runs'"
        ).fetchall()
    )
    assert {
        name: columns[name]
        for name in (
            "valid_count",
            "unchanged_count",
            "validation_summary",
            "source_metadata",
            "staging_recovery_required",
        )
    } == {
        "valid_count": "int8",
        "unchanged_count": "int8",
        "validation_summary": "jsonb",
        "source_metadata": "jsonb",
        "staging_recovery_required": "bool",
    }
    assert db.execute(
        "SELECT type,srid FROM geometry_columns WHERE f_table_schema='silver' "
        "AND f_table_name='tax_lots' AND f_geometry_column='geom'"
    ).fetchone() == ("MULTIPOLYGON", 2263)


@pytest.mark.parametrize("column", ["valid_count", "unchanged_count"])
def test_import_accounting_constraints(db, column):
    import psycopg
    from psycopg import sql

    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(
            sql.SQL(
                "INSERT INTO etl.import_runs(dataset_name,pipeline_version,{}) "
                "VALUES ('MapPLUTO','test',-1)"
            ).format(sql.Identifier(column))
        )
