import csv
import logging
import json
import os
import sys
import polars as pl
import polars.datatypes as dt
from enum import Enum
from argparse import ArgumentParser
from glob import glob
from sqlalchemy import *


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
        self.from_pools = from_pools
        self.to_pools = to_pools
        self.flux_source = flux_source


def read_flux_indicators(indicator_config):
    pool_collections = indicator_config["pool_collections"]
    fluxes = [
        FluxIndicator(
            flux, pool_collections[details["from"]], pool_collections[details["to"]],
            FluxSource.from_string(details["source"]))
        for flux, details in indicator_config["fluxes"].items()
    ]

    return fluxes


def merge(base_results_path, name):
    results_group_path = os.path.join(base_results_path, name)
    if not os.path.exists(results_group_path):
        return false

    header = None
    dtype = None
    sum_cols = None
    groupby_cols = None
    
    for year in (
        d for d in os.listdir(results_group_path)
        if os.path.isdir(os.path.join(results_group_path, d))
    ):
        output_path = os.path.join(base_results_path, f"{name}_{year}.parquet")
        if os.path.exists(output_path):
            continue

        year_dir = os.path.join(results_group_path, year)
        year_file_pattern = os.path.join(year_dir, "*.csv")
        if header is None:
            csv_files = glob(year_file_pattern)
            header = next(csv.reader(open(csv_files[0])))
            dtype = {
                c: dt.Float32 if c in ("area", "flux_tc", "pool_tc")
                   else dt.Int32 if c in ("year", "disturbance_code")
                   else str
                for c in header
            }
            
            sum_cols = [c for c in header if c in ("area", "flux_tc", "pool_tc")]
            groupby_cols = list(set(header) - set(sum_cols))

        pl.scan_csv(year_file_pattern, dtypes=dtype, null_values="").groupby(groupby_cols).agg([
            pl.sum(col) for col in sum_cols
        ]).collect().write_parquet(output_path)

    return True


def df_to_table(data, db_path, table_name):
    output_db_engine = create_engine(f"sqlite:///{db_path}")
    conn = output_db_engine.connect()
    
    for sql in (
        "PRAGMA main.cache_size=-1000000",
        "PRAGMA synchronous=OFF",
        "PRAGMA journal_mode=OFF"
    ):
        conn.execute(sql)

    with conn.begin():
        col_types = [
            (c, Numeric if c in ("area", "flux_tc", "pool_tc")
               else Integer if c in ("year", "disturbance_code")
               else Text)
            for c in data.columns
        ]
    
        md = MetaData()
        table = Table(table_name, md,
            *(Column(c, dt) for (c, dt) in col_types)
        )
        
        table.create(conn, checkfirst=True)
        
        slice_size = 1000000
        slice_idx = 0
        slice = data.slice(slice_idx, slice_size).collect()
        while len(slice) > 0:
            slice.to_pandas().to_sql(table_name, conn, index=False, if_exists="append")
            slice_idx += slice_size
            slice = data.slice(slice_idx, slice_size).collect()


def compile_flux_indicators(merged_flux_data, indicators, output_db):
    flux_indicators = read_flux_indicators(indicators)

    groupby_columns = list(set(merged_flux_data.columns)
        - {"flux_tc", "from_pool", "to_pool"}
        - set(c for c in merged_flux_data.columns if c.endswith("previous")))
        
    for flux in flux_indicators:
        if flux.flux_source == FluxSource.Disturbance:
            selection = merged_flux_data.filter((
                ((pl.col("disturbance_type").is_not_null()) & (pl.col("disturbance_type") != ""))
                & (pl.col("from_pool").is_in(flux.from_pools))
                & (pl.col("to_pool").is_in(flux.to_pools))))
        elif flux.flux_source == FluxSource.AnnualProcess:
            selection = merged_flux_data.filter((
                ((pl.col("disturbance_type").is_null()) | (pl.col("disturbance_type") == ""))
                & (pl.col("from_pool").is_in(flux.from_pools))
                & (pl.col("to_pool").is_in(flux.to_pools))))
        else:
            selection = merged_flux_data.filter((
                (pl.col("from_pool").is_in(flux.from_pools))
                & (pl.col("to_pool").is_in(flux.to_pools))))
        
        flux_data = selection.groupby(groupby_columns).agg([pl.sum("flux_tc")]).select([
            pl.all(),
            pl.lit(flux.name).alias("indicator")
        ])

        df_to_table(flux_data, output_db, "v_flux_indicators")


def compile_flux_indicator_aggregates(base_columns, indicator_config, output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    with conn.begin():
        md = MetaData()
        table = Table("v_flux_indicator_aggregates", md,
            Column("indicator", Text),
            Column("year", Integer),
            *(Column(c, Text) for c in base_columns - {"year"}),
            Column("flux_tc", Numeric)
        )
        
        table.create(output_db_engine, checkfirst=True)

        for name, flux_indicators in indicator_config["flux_collections"].items():
            conn.execute(
                f"""
                INSERT INTO v_flux_indicator_aggregates (indicator, {','.join(base_columns)}, flux_tc)
                SELECT ? AS aggregate_indicator, {','.join(base_columns)}, SUM(flux_tc)
                FROM v_flux_indicators
                WHERE indicator IN ({','.join('?' * len(flux_indicators))})
                GROUP BY {','.join(base_columns)}
                """, [name] + flux_indicators)


def compile_stock_change_indicators(base_columns, indicator_config, output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    with conn.begin():
        md = MetaData()
        table = Table("v_stock_change_indicators", md,
            Column("indicator", Text),
            Column("year", Integer),
            *(Column(c, Text) for c in base_columns - {"year"}),
            Column("flux_tc", Numeric)
        )
        
        table.create(output_db_engine, checkfirst=True)

        for name, components in indicator_config["stock_changes"].items():
            add_sub_sql = []
            add_sub_params = []
            for sign, flux_aggregates in components.items():
                mult = 1 if sign == "+" else -1 if sign == "-" else "err"
                add_sub_sql.append(f"WHEN indicator IN ({','.join('?' * len(flux_aggregates))}) THEN {mult}")
                add_sub_params.extend(flux_aggregates)
            
            unique_aggregates = list(set(add_sub_params))
            
            conn.execute(
                f"""
                INSERT INTO v_stock_change_indicators (indicator, {','.join(base_columns)}, flux_tc)
                SELECT
                    ? AS stock_change_indicator,
                    {','.join(base_columns)},
                    SUM(flux_tc * CASE {' '.join(add_sub_sql)} END)
                FROM v_flux_indicator_aggregates
                WHERE indicator IN ({','.join('?' * len(unique_aggregates))})
                GROUP BY {','.join(base_columns)}
                """, [name] + add_sub_params + unique_aggregates)


def compile_pool_indicators(base_columns, indicator_config, output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    with conn.begin():
        md = MetaData()
        table = Table("v_pool_indicators", md,
            Column("indicator", Text),
            Column("year", Integer),
            *(Column(c, Text) for c in base_columns - {"year"}),
            Column("pool_tc", Numeric)
        )
        
        table.create(output_db_engine, checkfirst=True)

        for name, pool_collection in indicator_config["pool_indicators"].items():
            pools = indicator_config["pool_collections"][pool_collection]
            conn.execute(
                f"""
                INSERT INTO v_pool_indicators (indicator, {','.join(base_columns)}, pool_tc)
                SELECT
                    ? AS pool_indicator,
                    {','.join(base_columns)},
                    SUM(pool_tc)
                FROM raw_pools
                WHERE pool IN ({','.join('?' * len(pools))})
                    AND year > 0
                GROUP BY {','.join(base_columns)}
                """, [name] + pools)


def create_views(output_db):
    output_db_engine = create_engine(f"sqlite:///{output_db}")
    conn = output_db_engine.connect()
    
    raw_flux_cols = {f'"{c}"' for c in (
        {c for c in conn.execute("SELECT * FROM raw_fluxes LIMIT 1").keys()}
        - {"age_range", "age_range_previous"})}
    
    raw_dist_cols = {f'"{c}"' for c in (
        {c for c in conn.execute("SELECT * FROM raw_fluxes LIMIT 1").keys()}
        - {"age_range", "age_range_previous", "flux_tc", "from_pool", "to_pool"})}
    
    with conn.begin():
        for sql in (
            "CREATE VIEW IF NOT EXISTS v_age_indicators AS SELECT * FROM raw_ages WHERE year > 0",
            "CREATE VIEW IF NOT EXISTS v_error_indicators AS SELECT * FROM raw_errors",
            f"""
            CREATE VIEW IF NOT EXISTS v_disturbance_fluxes AS
            SELECT
                {','.join(raw_flux_cols)},
                age_range_previous AS pre_dist_age_range,
                age_range AS post_dist_age_range
            FROM raw_fluxes
            WHERE disturbance_type IS NOT NULL
            """,
            f"""
            CREATE VIEW IF NOT EXISTS v_disturbance_indicators AS
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
                WHERE disturbance_type IS NOT NULL
                GROUP BY {','.join(raw_dist_cols)}, age_range, age_range_previous
            ) AS f
            ON {' AND '.join((f'd.{c} = f.{c}' for c in raw_dist_cols))}
                AND d.age_range = f.age_range
                AND d.age_range_previous = f.age_range_previous
            """,
            f"""
            CREATE VIEW IF NOT EXISTS v_total_disturbed_areas AS
            SELECT
                {','.join(raw_dist_cols)},
                SUM(area) AS dist_area
            FROM raw_disturbances
            GROUP BY {','.join(raw_dist_cols)}
            """,
        ):
            conn.execute(sql)


def compile_gcbm_output(results_path, output_db, indicator_config_file=None):
    output_dir = os.path.dirname(output_db)
    os.makedirs(output_dir, exist_ok=True)
    
    if os.path.exists(output_db):
        os.remove(output_db)
    
    if not merge(results_path, "age"):
        logging.info(f"No results to process in {results_path}")
        return
    
    base_columns = None
    for age_output_file in glob(os.path.join(results_path, "age_*.parquet")):
        data = pl.scan_parquet(age_output_file)
        if not base_columns:
            base_columns = set(data.columns) - {"area"}
        
        df_to_table(data, output_db, "raw_ages")
        data = None

    indicators = json.load(open(
        indicator_config_file
        or os.path.join(os.path.dirname(__file__), "compileresults.json")))
    
    if merge(results_path, "disturbance"):
        for dist_output_file in glob(os.path.join(results_path, "disturbance_*.parquet")):
            data = pl.scan_parquet(dist_output_file)
            df_to_table(data, output_db, "raw_disturbances")
            data = None

    if merge(results_path, "flux"):
        for flux_output_file in glob(os.path.join(results_path, "flux_*.parquet")):
            data = pl.scan_parquet(flux_output_file)
            df_to_table(data, output_db, "raw_fluxes")
            compile_flux_indicators(data, indicators, output_db)
            data = None

        compile_flux_indicator_aggregates(base_columns, indicators, output_db)
        compile_stock_change_indicators(base_columns, indicators, output_db)

    if merge(results_path, "pool"):
        for pool_output_file in glob(os.path.join(results_path, "pool_*.parquet")):
            data = pl.scan_parquet(pool_output_file)
            df_to_table(data, output_db, "raw_pools")
            data = None

        compile_pool_indicators(base_columns, indicators, output_db)

    if merge(results_path, "error"):
        for error_output_file in glob(os.path.join(results_path, "error_*.parquet")):
            data = pl.scan_parquet(error_output_file)
            df_to_table(data, output_db, "raw_errors")
            data = None

    create_views(output_db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(message)s",
                        datefmt="%m/%d %H:%M:%S")

    parser = ArgumentParser(description="Produce reporting tables from GCBM CSV results.")

    parser.add_argument("results_path",       help="path to CSV output files", type=os.path.abspath)
    parser.add_argument("output_db",          help="path to the database to write results tables to", type=os.path.abspath)
    parser.add_argument("--indicator_config", help="indicator configuration file - defaults to a generic set")
    args = parser.parse_args()
    
    compile_gcbm_output(args.results_path, args.output_db, args.indicator_config)
