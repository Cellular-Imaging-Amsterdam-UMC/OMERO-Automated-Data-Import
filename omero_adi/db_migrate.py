import os
import pathlib
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

MIGRATIONS_DIR = str(pathlib.Path(__file__).with_name("migrations"))
VERSION_TABLE = "alembic_version_omeroadi"


def run_migrations_on_startup():
    if os.getenv("ADI_RUN_MIGRATIONS", "1") != "1":
        return

    db_url = os.getenv("INGEST_TRACKING_DB_URL",
                       "sqlite:///ingestion_tracking.db")
    engine = create_engine(db_url)

    cfg = Config()
    cfg.set_main_option("script_location", MIGRATIONS_DIR)
    cfg.set_main_option("sqlalchemy.url", db_url)
    cfg.set_main_option("version_table", VERSION_TABLE)

    insp = inspect(engine)
    has_version_table = insp.has_table(VERSION_TABLE)
    # Heuristic: if we already have ADI tables but no version table, allow auto-stamp
    # so existing installs can adopt Alembic without recreating tables.
    # Disable by setting ADI_ALLOW_AUTO_STAMP=0
    # Default to not auto-stamping so first real migrations actually apply.
    # Enable explicitly with ADI_ALLOW_AUTO_STAMP=1 if you are adopting Alembic
    # on a database that already matches the head schema.
    allow_stamp = os.getenv("ADI_ALLOW_AUTO_STAMP", "0") == "1"

    # Postgres advisory lock to prevent concurrent migrations from multiple replicas
    is_pg = engine.dialect.name == "postgresql"

    with engine.begin() as conn:
        if is_pg:
            conn.execute(
                text("SELECT pg_advisory_lock(hashtext('omeroadi_migrations'))"))
        try:
            if allow_stamp and not has_version_table:
                # Check if any ADI table already exists (replace with a reliable table name)
                known_tables = {"imports"}  # add/adjust if needed
                if any(insp.has_table(t) for t in known_tables):
                    command.stamp(cfg, "head")  # baseline existing DB
            command.upgrade(cfg, "head")
        finally:
            if is_pg:
                conn.execute(
                    text("SELECT pg_advisory_unlock(hashtext('omeroadi_migrations'))"))
