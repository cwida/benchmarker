


import os
from typing import List

import duckdb

from src.logger import get_logger
from src.models import DataSet, Benchmark, Query
from src.utils import get_data_path, pad

logger = get_logger(__name__)

def get_join_build_table_name(cardinality: int, duplicates: int) -> str:
    return f"join_build_{cardinality}_{duplicates}"

# *** JOIN DUPS MICRO BENCHMARK ***

JOIN_BUILD_WITH_DUPS_DUPS = [1, 16, 256, 4096, 65536, 1048576, 16777216]
JOIN_BUILD_WITH_DUPS_CARDINALITY = 100_000_000 # 100M rows in the build table

JOIN_MICRO_BUILD_QUERIES_DUPS: List[Query] = [
    {
        'name': f'join_micro_build_{dup}',
        'index': i,
        'run_script': {
            "duckdb": f"SELECT * FROM probe JOIN {get_join_build_table_name(JOIN_BUILD_WITH_DUPS_CARDINALITY, dup)} as build ON probe.key = build.key;"
        }
    } for i, dup in enumerate(JOIN_BUILD_WITH_DUPS_DUPS)
]

def get_join_micro_build_benchmark() -> Benchmark:

    datasets: List[DataSet] = __generate_and_return_data()

    queries = JOIN_MICRO_BUILD_QUERIES_DUPS
    return {
        'name': 'join_micro_build',
        'datasets': datasets,
        'queries': queries
    }


def __get_file_path() -> str:
    file_name =  os.path.join('join', f'micro_build.db')
    return get_data_path(file_name)


def __generate_and_return_data() -> List[DataSet]:
    __generate_data()

    duckdb_file_path = __get_file_path()
    duckdb_file_name_without_extension = os.path.splitext(os.path.basename(duckdb_file_path))[0]


    setup_script = {
        'duckdb': f"ATTACH '{duckdb_file_path}' (READ_ONLY); USE '{duckdb_file_name_without_extension}'; PRAGMA disable_optimizer; PRAGMA disable_progress_bar;"
    }

    dataset: DataSet = {
        'name': f'join-micro-build',
        'setup_script': setup_script,
        'config': {}
    }

    return [dataset]


def __generate_data():

    logger.info(f'Generating data for Join Micro Benchmark...')
    duckdb_file_path = __get_file_path()

    # only generate the data if the file does not exist
    if os.path.exists(duckdb_file_path):
        logger.info(f'File {duckdb_file_path} already exists, finished ...')
        return
    else:
        logger.info(f'File {duckdb_file_path} does not exist, generating...')

    con = duckdb.connect(duckdb_file_path)
    data_gen_query = f"CREATE TABLE probe (key INT64);SELECT setseed(0.42);"
    for dup in JOIN_BUILD_WITH_DUPS_DUPS:
        table_name = get_join_build_table_name(JOIN_BUILD_WITH_DUPS_CARDINALITY, dup)
        data_gen_query += f"CREATE TABLE {table_name} AS SELECT range // {dup} as key FROM range({JOIN_BUILD_WITH_DUPS_CARDINALITY}) ORDER BY random();"
    con.sql(data_gen_query)
    con.close()