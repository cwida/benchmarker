import os
from typing import List

import duckdb

from src.logger import get_logger
from src.models import DataSet, Benchmark, Query
from src.utils import get_data_path, pad

logger = get_logger(__name__)

TPC_DS_QUERIES: List[Query] = [
    {
        'name': f'tpcds{i + 1}',
        'index': i,
        'run_script': {
            "duckdb": f"PRAGMA tpcds({i + 1});",
        }
    } for i in range(99)
]

def get_tpcds_benchmark(scale_factors: List[int]) -> Benchmark:

    datasets: List[DataSet] = __generate_and_return_tpc_data(scale_factors)

    queries = TPC_DS_QUERIES

    return {
        'name': 'tpcds',
        'datasets': datasets,
        'queries': queries
    }


def __get_tpcds_file_path(sf: int) -> str:
    file_name =  os.path.join('tpcds', f'tpcds-sf-{sf}.db')
    return get_data_path(file_name)


def __generate_and_return_tpc_data(sfs: List[int]) -> List[DataSet]:
    __generate_tpcds_data(sfs)

    datasets: List[DataSet] = []
    for sf in sfs:
        duckdb_file_path = __get_tpcds_file_path(sf)
        duckdb_file_name_without_extension = os.path.splitext(os.path.basename(duckdb_file_path))[0]

        setup_script = {
            'duckdb': f"ATTACH '{duckdb_file_path}' (READ_ONLY); USE '{duckdb_file_name_without_extension}';"
        }

        dataset: DataSet = {
            'name': f'tpcds-{sf}',
            'setup_script': setup_script,
            'config': {
                'sf': sf
            }
        }

        datasets.append(dataset)

    return datasets


def __generate_tpcds_data(sfs: List[int]):
    for (index, sf) in enumerate(sfs):
        logger.info(f'Generating data for TPC-DS scale factor {sf} ({index + 1}/{len(sfs)}) ...')
        duckdb_file_path = __get_tpcds_file_path(sf)
        # only generate the data if the file does not exist
        if os.path.exists(duckdb_file_path):
            logger.info(f'File {duckdb_file_path} already exists, skipping...')
            continue
        else:
            logger.info(f'File {duckdb_file_path} does not exist, generating...')

        logger.info(f'Started to generate data for TPC-DS scale factor {sf} ...')

        con = duckdb.connect(duckdb_file_path)
        query_tpcds = f"""
            INSTALL tpcds;
            LOAD tpcds;
            CALL dsdgen(sf = {sf});
        """
        con.sql(query_tpcds)
        con.close()