


import os
import unittest
from typing import List, Tuple

import duckdb

from src.logger import get_logger
from src.models import DataSet, Benchmark, Query
from src.utils import get_data_path, pad

logger = get_logger(__name__)
# *** JOIN DUPS MICRO BENCHMARK ***
JOIN_PROBE_CARDINALITIES = [10_000_000, 100_000_000]  # 1M to 1B rows in the probe table
JOIN_BUILD_TO_PROBE_RATIO = 10.0
JOIN_PROBE_SELECTIVITIES = [0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 0.75, 1.0]


class BenchmarkConfigMicroSelectivity:
    def __init__(self, selectivity: float, probe_cardinality: int):
        self.probe_size = probe_cardinality
        self.selectivity = selectivity
        self.expected_cardinality = int(self.probe_size * self.selectivity)


    def get_join_build_table_name(self) -> str:
        return f"join_build_{self.probe_size}_{int(self.selectivity * 1000)}"

    def get_join_probe_table_name(self) -> str:
        return f"join_probe_{self.probe_size}"

    def get_queries(self)  -> Tuple[Tuple[str, str], Tuple[str, str]]:
        build_table_name = self.get_join_build_table_name()
        probe_table_name = self.get_join_probe_table_name()

        probe_table_string = f"CREATE TABLE IF NOT EXISTS {probe_table_name} AS SELECT (range) AS key FROM range({self.probe_size}) ORDER BY random()"
        build_table_string = f"CREATE TABLE IF NOT EXISTS {build_table_name} AS SELECT key FROM {probe_table_name} WHERE random() < {self.selectivity}"

        return (probe_table_name, build_table_name), (probe_table_string, build_table_string)


def get_join_micro_probe_sel_benchmark() -> Benchmark:

    datasets: List[DataSet] = __generate_and_return_data()

    queries = []

    logger.info(f'Generating data for Join Micro Benchmark...')
    duckdb_file_path = __get_file_path()

    # only generate the data if the file does not exist
    if os.path.exists(duckdb_file_path):
        logger.info(f'File {duckdb_file_path} already exists, finished ...')
        needs_to_generate = False
    else:
        logger.info(f'File {duckdb_file_path} does not exist, generating...')
        needs_to_generate = True

    n_total_queries = len(JOIN_PROBE_SELECTIVITIES) * len(JOIN_PROBE_CARDINALITIES)
    n_created_queries = 0

    # set the seed for reproducibility
    con = duckdb.connect(duckdb_file_path)
    con.execute(f"SELECT setseed(0.42);")
    for selectivity in JOIN_PROBE_SELECTIVITIES:
        for probe_cardinality in JOIN_PROBE_CARDINALITIES:
            selectivity_int = round(1000 * selectivity)
            build_to_probe_ratio_int = round(JOIN_BUILD_TO_PROBE_RATIO)
            config = BenchmarkConfigMicroSelectivity(selectivity, probe_cardinality)
            (p_name, b_name), (p_query, b_query) = config.get_queries()

            expected_cardinality = config.expected_cardinality
            if expected_cardinality > 1_000_000_000:
                logger.warning(f"Expected result cardinality is too high: {expected_cardinality}. Skipping query.")
                continue

            query = {
                'name': f'join_micro_probe_{build_to_probe_ratio_int}_{selectivity_int}_{probe_cardinality}',
                'index': len(queries),
                'run_script': {
                    "duckdb": f"SELECT * FROM {p_name} as probe JOIN {b_name} as build ON probe.key = build.key AND hash(build.key) > 1;"
                },
                'config': {
                    'build_to_probe_ratio': JOIN_BUILD_TO_PROBE_RATIO,
                    'selectivity': selectivity,
                    'probe_cardinality': probe_cardinality,
                    'expected_cardinality': config.expected_cardinality,
                    'build_table_query': b_query,
                    'probe_table_query': p_query,
                },
            }
            queries.append(query)

            if needs_to_generate:
                con.execute(p_query)
                con.execute(b_query)

            if n_created_queries % 10 == 0:
                logger.info(f"Created {n_created_queries} out of {n_total_queries} queries.")
            n_created_queries += 1

    con.close()

    return {
        'name': 'join_micro_probe_selectivity',
        'datasets': datasets,
        'queries': queries
    }


def __get_file_path() -> str:
    file_name =  os.path.join('join', f'micro_probe_selectivity.db')
    return get_data_path(file_name)


def __generate_and_return_data() -> List[DataSet]:

    duckdb_file_path = __get_file_path()
    duckdb_file_name_without_extension = os.path.splitext(os.path.basename(duckdb_file_path))[0]


    setup_script = {
        'duckdb': f"ATTACH '{duckdb_file_path}' (READ_ONLY); USE '{duckdb_file_name_without_extension}'; PRAGMA disable_progress_bar;"
    }

    dataset: DataSet = {
        'name': f'join-micro-probe-selectivity',
        'setup_script': setup_script,
        'config': {}
    }

    return [dataset]


if __name__ == "__main__":
    # This is just for testing the data generation
    get_join_micro_probe_sel_benchmark()
    logger.info("Data generation finished.")