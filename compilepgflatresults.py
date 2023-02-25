import logging
import json
import os
import sys
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from enum import Enum
from argparse import ArgumentParser
from sqlalchemy import *


ANY_POOL = "Any"


class FluxSource(Enum):
    
    Disturbance   = 1
    AnnualProcess = 2
    Any           = 3
    
    @staticmethod
    def from_string(str):
        return (
            FluxSource.Disturbance if str == "Disturbance"
            else FluxSource.AnnualProcess if str == "Annual Process"
            else FluxSource.Any
        )


class FluxIndicator:

    def __init__(self, name, from_pools, to_pools, flux_source):
        self.name = name
        self.from_pools = tuple(from_pools)
        self.to_pools = tuple(to_pools)
        self.flux_source = flux_source


def table_exists(conn, schema, table):
    return conn.execute(text(
        """
        SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE schemaname = :schema
                AND tablename = :table
        )
        """).bindparams(schema=schema, table=table)).fetchone()[0]


def copy_reporting_tables(from_conn, from_schema, to_conn, to_schema=None, include_raw_tables=False):
    for sql in (
        "PRAGMA main.cache_size=-1000000",
        "PRAGMA synchronous=OFF",
        "PRAGMA journal_mode=OFF"
    ):
        to_conn.execute(text(sql))
    
    md = MetaData()
    md.reflect(bind=from_conn, schema=from_schema, views=True)
    output_md = MetaData(schema=to_schema)
    for fqn, table in md.tables.items():
        if ((not include_raw_tables and table.name.startswith("raw"))
            or table.name in ("run_status", "completedjobs")
        ):
            continue
        
        table.to_metadata(output_md, schema=None)
        output_table = Table(table.name, output_md)
        output_table.drop(to_conn, checkfirst=True)
        output_table.create(to_conn)
        
        batch = []
        for i, row in enumerate(from_conn.execute(select(table))):
            batch.append({k: v for k, v in row._mapping.items()})
            if i % 50000 == 0:
                to_conn.execute(insert(output_table), batch)
                to_conn.commit()
                batch = []
        
        if batch:
            to_conn.execute(insert(output_table), batch)
            to_conn.commit()
        

def read_flux_indicators(indicator_config):
    pool_collections = indicator_config["pool_collections"]
    pool_collections[ANY_POOL] = [ANY_POOL]
    
    fluxes = [
        FluxIndicator(
            flux, pool_collections[details["from"]], pool_collections[details["to"]],
            FluxSource.from_string(details["source"]))
        for flux, details in indicator_config["fluxes"].items()
    ]

    return fluxes


def compile_flux_indicators(conn, schema, indicator_config, classifiers):
    flux_indicators = read_flux_indicators(indicator_config)
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    conn.execute(text(f"SET SEARCH_PATH={schema}"))
    conn.execute(text("DROP TABLE IF EXISTS v_flux_indicators"))
    conn.execute(text(
        f"""
        CREATE UNLOGGED TABLE v_flux_indicators (
            indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR, unfccc_land_class VARCHAR,
            age_range VARCHAR, disturbance_type VARCHAR, disturbance_code INTEGER, flux_tc NUMERIC)
        """))
    
    for flux in flux_indicators:
        disturbance_filter = (
            "disturbance_type IS NOT NULL AND disturbance_type <> ''"
                if flux.flux_source == FluxSource.Disturbance
            else "disturbance_type IS NULL OR disturbance_type = ''"
                if flux.flux_source == FluxSource.AnnualProcess
            else "1=1"
        )
        
        pool_filters = []
        pool_filter_params = {}
        if ANY_POOL in flux.from_pools:
            pool_filter.append("from_pool IN :from_pools")
            pool_filter_params["from_pools"] = flux.from_pools
        if ANY_POOL in flux.to_pools:
            pool_filter.append("to_pool IN :to_pools")
            pool_filter_params["to_pools"] = flux.to_pools
            
        pool_filter_sql = " AND ".join(pool_filters)
    
        sql = \
            f"""
            INSERT INTO v_flux_indicators (
                indicator, year, {classifiers_select}, unfccc_land_class, age_range,
                disturbance_type, disturbance_code, flux_tc)
            SELECT :name AS indicator, year, {classifiers_select}, unfccc_land_class,
                age_range, disturbance_type, disturbance_code, SUM(flux_tc) AS flux_tc
            FROM raw_fluxes
            WHERE ({disturbance_filter})
                {('AND ' + pool_filter_sql) if pool_filter_sql else ''}
            GROUP BY year, {classifiers_select}, unfccc_land_class, age_range,
                disturbance_type, disturbance_code
            """
            
        conn.execute(text(sql).bindparams(name=flux.name, **pool_filter_params))
    
    conn.commit()

def compile_flux_indicator_aggregates(conn, schema, indicator_config, classifiers):
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    conn.execute(text(f"SET SEARCH_PATH={schema}"))
    conn.execute(text("DROP TABLE IF EXISTS v_flux_indicator_aggregates"))
    conn.execute(text(
        f"""
        CREATE UNLOGGED TABLE v_flux_indicator_aggregates (
            indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR,
            unfccc_land_class VARCHAR, age_range VARCHAR, flux_tc NUMERIC)
        """))
    
    for name, flux_indicators in indicator_config["flux_collections"].items():
        conn.execute(text(
            f"""
            INSERT INTO v_flux_indicator_aggregates (
                indicator, year, {classifiers_select}, unfccc_land_class, age_range,
                flux_tc)
            SELECT
                :name AS indicator, year, {classifiers_select}, unfccc_land_class,
                age_range, SUM(flux_tc) AS flux_tc
            FROM v_flux_indicators
            WHERE indicator IN :flux_indicators
            GROUP BY year, {classifiers_select}, unfccc_land_class, age_range
            """).bindparams(name=name, flux_indicators=tuple(flux_indicators)))

    conn.commit()

def compile_stock_change_indicators(conn, schema, indicator_config, classifiers):
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    conn.execute(text(f"SET SEARCH_PATH={schema}"))
    conn.execute(text("DROP TABLE IF EXISTS v_stock_change_indicators"))
    conn.execute(text(
        f"""
        CREATE UNLOGGED TABLE v_stock_change_indicators (
            indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR,
            unfccc_land_class VARCHAR, age_range VARCHAR, flux_tc NUMERIC)
        """))

    for name, components in indicator_config["stock_changes"].items():
        query_params = {"name": name}
        unique_aggregates = set()

        add_sub_sql = []
        for i, (sign, flux_aggregates) in enumerate(components.items()):
            mult = 1 if sign == '+' else -1 if sign == '-' else 'err'
            add_sub_sql.append(f"WHEN indicator IN :fluxes_{i} THEN {mult}")
            unique_aggregates.update(flux_aggregates)
            query_params[f"fluxes_{i}"] = tuple(flux_aggregates)
        
        query_params["unique_aggregates"] = tuple(unique_aggregates)
        
        conn.execute(text(
            f"""
            INSERT INTO v_stock_change_indicators (
                indicator, year, {classifiers_select}, unfccc_land_class,
                age_range, flux_tc)
            SELECT
                :name AS indicator, year, {classifiers_select}, unfccc_land_class, age_range,
                SUM(flux_tc * CASE {' '.join(add_sub_sql)} END) AS flux_tc
            FROM v_flux_indicator_aggregates
            WHERE indicator IN :unique_aggregates
            GROUP BY year, {classifiers_select}, unfccc_land_class, age_range
            """).bindparams(**query_params))
    
    conn.commit()


def compile_pool_indicators(conn, schema, indicator_config, classifiers, spinup_year=0):
    classifiers_ddl = " VARCHAR, ".join(classifiers)
    classifiers_select = ",".join(classifiers)

    conn.execute(text(f"SET SEARCH_PATH={schema}"))
    conn.execute(text("DROP TABLE IF EXISTS v_pool_indicators"))
    conn.execute(text(
        f"""
        CREATE UNLOGGED TABLE v_pool_indicators (
            indicator VARCHAR, year INTEGER, {classifiers_ddl} VARCHAR,
            unfccc_land_class VARCHAR, age_range VARCHAR, pool_tc NUMERIC)
        """))
    
    for name, pool_collection in indicator_config["pool_indicators"].items():
        pools = tuple(indicator_config["pool_collections"][pool_collection])
        conn.execute(text(
            f"""
            INSERT INTO v_pool_indicators (
                indicator, year, {classifiers_select}, unfccc_land_class,
                age_range, pool_tc)
            SELECT
                :name AS indicator, CASE WHEN year <= 0 THEN {spinup_year} ELSE year END,
                {classifiers_select}, unfccc_land_class, age_range, SUM(pool_tc)
            FROM raw_pools
            WHERE pool IN :pools
            GROUP BY year, {classifiers_select}, unfccc_land_class, age_range
            """).bindparams(name=name, pools=pools))

    conn.commit()

def create_views(conn, schema=None, disturbance_views=True, error_views=True, spinup_year=0):
    view_sql = []
    if schema:
        # If schema is specified, then executing against Postgres.
        view_sql.append(f"SET SEARCH_PATH={schema}")

    view_sql.extend([f"DROP VIEW IF EXISTS {view}" for view in (
        "v_age_indicators", "v_error_indicators", "v_disturbance_fluxes",
        "v_disturbance_indicators", "v_total_disturbed_areas"
    )])
    
    raw_ages_fqn = f"{schema}.raw_ages" if schema else "raw_ages"
    raw_age_cols = {f'"{c}"' for c in (
        {c for c in conn.execute(text(f"SELECT * FROM {raw_ages_fqn} LIMIT 1")).keys()}
        - {"year"})}
        
    view_sql.append(
        f"""
        CREATE VIEW v_age_indicators AS
        SELECT
            CASE WHEN year <= 0 THEN {spinup_year} ELSE year END AS year,
            {','.join(raw_age_cols)}
        FROM raw_ages
        """)
    
    if error_views:
        view_sql.append("CREATE VIEW v_error_indicators AS SELECT * FROM raw_errors")
    
    if disturbance_views:
        raw_fluxes_fqn = f"{schema}.raw_fluxes" if schema else "raw_fluxes"
        raw_flux_cols = {f'"{c}"' for c in (
            {c for c in conn.execute(text(f"SELECT * FROM {raw_fluxes_fqn} LIMIT 1")).keys()}
            - {"age_range", "age_range_previous"})}
        
        raw_dist_cols = {f'"{c}"' for c in (
            {c for c in conn.execute(text(f"SELECT * FROM {raw_fluxes_fqn} LIMIT 1")).keys()}
            - {"age_range", "age_range_previous", "flux_tc", "from_pool", "to_pool"})}
        
        view_sql.extend([
            f"""
            CREATE VIEW v_disturbance_fluxes AS
            SELECT
                {','.join(raw_flux_cols)},
                age_range_previous AS pre_dist_age_range,
                age_range AS post_dist_age_range
            FROM raw_fluxes
            WHERE (disturbance_type IS NOT NULL AND disturbance_type <> '')
            """,
            f"""
            CREATE VIEW v_disturbance_indicators AS
            SELECT
                {','.join((f'd.{c}' for c in raw_dist_cols))},
                d.age_range_previous AS pre_dist_age_range,
                d.age_range AS post_dist_age_range,
                d.area AS dist_area,
                f.flux_tc AS dist_carbon,
                f.flux_tc / d.area AS dist_carbon_per_ha
            FROM raw_disturbances d
            INNER JOIN (
                SELECT
                    {','.join(raw_dist_cols)},
                    age_range_previous,
                    age_range,
                    SUM(flux_tc) AS flux_tc
                FROM raw_fluxes
                WHERE (disturbance_type IS NOT NULL AND disturbance_type <> '')
                GROUP BY {','.join(raw_dist_cols)}, age_range, age_range_previous
            ) AS f
            ON {' AND '.join((f'd.{c} = f.{c}' for c in raw_dist_cols))}
                AND d.age_range = f.age_range
                AND d.age_range_previous = f.age_range_previous
            """,
            f"""
            CREATE VIEW v_total_disturbed_areas AS
            SELECT
                {','.join(raw_dist_cols)},
                SUM(area) AS dist_area
            FROM raw_disturbances
            GROUP BY {','.join(raw_dist_cols)}
            """
        ])

    for sql in view_sql:
        conn.execute(text(sql))

    conn.commit()

def compile_gcbm_output(title, conn_str, results_path, output_db, indicator_config_file=None,
                        chunk_size=1000, drop_schema=False, include_raw_tables=False):
    
    output_dir = os.path.dirname(output_db)
    os.makedirs(output_dir, exist_ok=True)
    
    if os.path.exists(output_db):
        os.remove(output_db)

    results_schema = title.lower()
    
    # Create the reporting tables in the simulation output schema.
    results_db_engine = create_engine(conn_str, future=True)
    with results_db_engine.connect() as conn:
        if drop_schema:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {results_schema} CASCADE"))
        
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {results_schema}"))
        conn.execute(text(f"SET SEARCH_PATH={results_schema}"))

        if not table_exists(conn, results_schema, "raw_ages"):
            return
        
        spinup_year = next(conn.execute(text(f"SELECT MIN(year) - 1 FROM raw_ages WHERE year > 0")))[0]

        classifier_names = {col for col in conn.execute(text("SELECT * FROM raw_ages LIMIT 1")).keys()} \
            - {"year", "unfccc_land_class", "age_range", "area"}

        classifiers = [f'"{c}"' for c in classifier_names]
        
        disturbance_views = table_exists(conn, results_schema, "raw_disturbances")
        error_views = table_exists(conn, results_schema, "raw_errors")
        
        indicators = json.load(open(
            indicator_config_file
            or os.path.join(os.path.dirname(__file__), "compileresults.json")))
        
        if table_exists(conn, results_schema, "raw_fluxes"):
            compile_flux_indicators(conn, results_schema, indicators, classifiers)
            compile_flux_indicator_aggregates(conn, results_schema, indicators, classifiers)
            compile_stock_change_indicators(conn, results_schema, indicators, classifiers)

        if table_exists(conn, results_schema, "raw_pools"):
            compile_pool_indicators(conn, results_schema, indicators, classifiers, spinup_year)

        if not include_raw_tables:
            create_views(conn, results_schema, disturbance_views, error_views, spinup_year)
        
    # Export the reporting tables to SQLite.
    with results_db_engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, max_row_buffer=50000)
        output_db_engine = create_engine(f"sqlite:///{output_db}", future=True)
        with output_db_engine.connect() as output_conn:
            copy_reporting_tables(conn, results_schema, output_conn,
                                  include_raw_tables=include_raw_tables)
    
    del output_db_engine
    del results_db_engine
    conn = None
    output_conn = None

    if include_raw_tables:
        output_db_engine = create_engine(f"sqlite:///{output_db}", future=True)
        conn = output_db_engine.connect()
        create_views(output_db, disturbance_views=disturbance_views, error_views=error_views,
                     spinup_year=spinup_year)
                     
        del output_db_engine
        conn = None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(message)s",
                        datefmt="%m/%d %H:%M:%S")

    parser = ArgumentParser(description="Produce reporting tables from GCBM CSV results.")

    parser.add_argument("title")
    parser.add_argument("results_conn_str",   help="SQLAlchemy connection string to Postgres database")
    parser.add_argument("results_path",       help="path to CSV output files", type=os.path.abspath)
    parser.add_argument("output_db",          help="path to the database to write results tables to", type=os.path.abspath)
    parser.add_argument("--indicator_config", help="indicator configuration file - defaults to a generic set")
    args = parser.parse_args()
    
    compile_gcbm_output(args.title, args.results_conn_str, args.results_path, args.output_db, args.indicator_config, True)
