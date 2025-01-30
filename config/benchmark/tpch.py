import logging
import os
from typing import List

import duckdb
from tqdm import tqdm

from src.models import DataSet, Benchmark
from src.utils import get_data_path, pad

TPC_H_QUERIES = [
    {
        'name': f'tpch{i + 1}',
        'run_script': {
            "duckdb": f"""
                PRAGMA tpch({i + 1});
          """,
        }
    } for i in range(22)
]

def get_tpch_benchmark(scale_factors: List[int]) -> Benchmark:

    datasets: List[DataSet] = __generate_and_return_tpc_data(scale_factors)

    queries = TPC_H_QUERIES

    return {
        'name': 'tpch',
        'datasets': datasets,
        'queries': queries
    }


def __get_tpch_file_path(sf: int) -> str:
    file_name =  f'tpc-h-sf-{sf}.duckdb'
    return get_data_path(file_name)


def __generate_and_return_tpc_data(sfs: List[int]) -> List[DataSet]:
    __generate_tpch_data(sfs)

    datasets: List[DataSet] = []
    for sf in sfs:
        duckdb_file_path = __get_tpch_file_path(sf)
        setup_script = {
            'duckdb': f".open '{duckdb_file_path}' --readonly ; \n"
        }

        dataset: DataSet = {
            'name': f'tpch-{sf}',
            'setup_script': setup_script,
            'config': {
                'sf': sf
            }
        }

        datasets.append(dataset)

    return datasets


def __generate_tpch_data(sfs: List[int]):
    for sf in tqdm(sfs, desc=pad(f'Generating TPC-H data')):
        duckdb_file_path = __get_tpch_file_path(sf)
        # only generate the data if the file does not exist
        if os.path.exists(duckdb_file_path):
            logging.info(f'File {duckdb_file_path} already exists, skipping...')
        else:
            logging.info(f'File {duckdb_file_path} does not exist, generating...')

        logging.info(f'Started to generate data for TPC-H scale factor {sf} ...')

        con = duckdb.connect(duckdb_file_path)
        query_tpch = f"""
            INSTALL tpch;
            LOAD tpch;
            CALL dbgen(sf = {sf});
        """
        con.sql(query_tpch)
        con.close()

        logging.info(f'Finished generating data for TPC-H scale factor {sf}!')