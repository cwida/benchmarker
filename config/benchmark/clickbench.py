import os
from typing import List

import duckdb

from src.logger import get_logger
from src.models import DataSet, Benchmark, Query
from src.utils import get_data_path, pad

logger = get_logger(__name__)


def get_clickbench_benchmark() -> Benchmark:
    datasets: List[DataSet] = __load_and_return_clickbench_data()
    queries = __get_clickbench_queries()

    return {
        'name': 'clickbench',
        'datasets': datasets,
        'queries': queries
    }

def __get_clickbench_file_path() -> str:
    file_name = os.path.join('clickbench', f'clickbench.db')
    return get_data_path(file_name)


def __get_clickbench_sql_file_path() -> str:
    # get the path of this source file
    source_file_path = os.path.abspath(__file__)
    # get the directory of the source file
    clickbench_sql_dir = os.path.join(os.path.dirname(source_file_path), 'clickbench')
    return clickbench_sql_dir

def __get_clickbench_queries() -> List[Query]:
    clickbench_sql_dir = __get_clickbench_sql_file_path()
    clickbench_queries_path = os.path.join(clickbench_sql_dir, 'queries')

    queries = []
    for file in os.listdir(clickbench_queries_path):
        if file.endswith('.sql'):
            with open(os.path.join(clickbench_queries_path, file), 'r') as f:
                query = f.read()
                file_name = os.path.basename(file)
                # remove the extension
                file_name_without_extension = os.path.splitext(file_name)[0]

                queries.append({
                    'name':file_name_without_extension,
                    'index': len(queries),
                    'run_script': {
                        "duckdb": query
                    }
                })

    logger.info(f'Loaded {len(queries)} queries from {clickbench_queries_path}')
    return queries


def __load_and_return_clickbench_data() -> List[DataSet]:
    __load_clickbench_data()

    duckdb_file_path = __get_clickbench_file_path()
    duckdb_file_name_without_extension = os.path.splitext(os.path.basename(duckdb_file_path))[0]

    setup_script = {
        'duckdb': f"ATTACH '{duckdb_file_path}' (READ_ONLY); USE '{duckdb_file_name_without_extension}'; PRAGMA disable_progress_bar;"
    }

    dataset: DataSet = {
        'name': f'clickbench',
        'setup_script': setup_script,
        'config': {}
    }


    return [dataset]


def __load_clickbench_data():
    logger.info(f'Downloading clickbench data...')
    duckdb_file_path = __get_clickbench_file_path()
    # only generate the data if the file does not exist
    if os.path.exists(duckdb_file_path):
        logger.info(f'File {duckdb_file_path} already exists, finished...')
    else:
        logger.info(f'Started to download data for clickbench...')

        con = duckdb.connect(duckdb_file_path)
        sql_path = __get_clickbench_sql_file_path()

        load_path = os.path.join(sql_path, 'schema', 'load.sql')
        load_sql = open(load_path, 'r').read()
        logger.info(f'Loading using {load_path}')
        con.sql(load_sql)
        con.close()
