


import os
import unittest
from typing import List, Tuple

import duckdb

from src.logger import get_logger
from src.models import DataSet, Benchmark, Query
from src.utils import get_data_path, pad

logger = get_logger(__name__)
# *** JOIN DUPS MICRO BENCHMARK ***
JOIN_PROBE_CARDINALITY = 10_000_000  # 100M rows in the probe table

JOIN_BUILD_TO_PROBE_RATIOS = [1.0, 4.0, 16.0, 64.0]
JOIN_BUILD_DUPS = [1, 4, 16, 64, 256, 1024, 4096, 16384]

JOIN_PROBE_SELECTIVITIES = [0.01, 0.1, 0.25, 0.5, 0.75, 1.0]


class BenchmarkConfig:
    def __init__(self, selectivity: float, build_to_probe_ratio: float, duplicates: int, probe_cardinality: int):
        self.probe_size = probe_cardinality
        self.build_size = int(round(probe_cardinality / build_to_probe_ratio))

        self.value_range = int(round(self.build_size / duplicates))
        self.probe_value_offset = int(round(self.value_range * (1 - selectivity)))

        self.expected_cardinality = int(round(self.probe_size * selectivity * duplicates))

    def get_join_build_table_name(self) -> str:
        return f"join_build_{self.value_range}_{self.build_size}"

    def get_join_probe_table_name(self) -> str:
        return f"join_probe_{self.value_range}_{self.probe_value_offset}"

    def get_queries(self)  -> Tuple[Tuple[str, str], Tuple[str, str]]:
        build_table_name = self.get_join_build_table_name()
        probe_table_name = self.get_join_probe_table_name()

        probe_table_string = f"CREATE TABLE IF NOT EXISTS {probe_table_name} AS SELECT (range % {self.value_range}) + {self.probe_value_offset} as key FROM range({self.probe_size}) ORDER BY random();"
        build_table_string = f"CREATE TABLE IF NOT EXISTS {build_table_name} AS SELECT range % {self.value_range} as key FROM range({self.build_size}) ORDER BY random();"

        return (probe_table_name, build_table_name), (probe_table_string, build_table_string)


class TestBenchmarkConfig(unittest.TestCase):

    def test_no_selectivity(self):
        selectivity = 1.0
        build_to_probe_ratio = 1.0
        duplicates = 1
        probe_cardinality = 100

        # expected values
        probe_size = 100
        build_size = 100
        value_range = 100
        probe_value_offset = 0

        # create the BenchmarkConfig object
        config = BenchmarkConfig(selectivity, build_to_probe_ratio, duplicates, probe_cardinality)
        self.assertEqual(config.probe_size, probe_size)
        self.assertEqual(config.build_size, build_size)
        self.assertEqual(config.value_range, value_range)
        self.assertEqual(config.probe_value_offset, probe_value_offset)

    def test_high_selectivity(self):
        selectivity = 0.01
        build_to_probe_ratio = 1.0
        duplicates = 1
        probe_cardinality = 100

        # expected values
        probe_size = 100
        build_size = 100
        value_range = 100
        probe_value_offset = 99

        # create the BenchmarkConfig object
        config = BenchmarkConfig(selectivity, build_to_probe_ratio, duplicates, probe_cardinality)
        self.assertEqual(config.probe_size, probe_size)
        self.assertEqual(config.build_size, build_size)
        self.assertEqual(config.value_range, value_range)
        self.assertEqual(config.probe_value_offset, probe_value_offset)

    def test_high_build_to_probe_ratio(self):
        selectivity = 0.1
        build_to_probe_ratio = 10.0
        duplicates = 1
        probe_cardinality = 100

        # expected values
        probe_size = 100
        build_size = 10
        value_range = 10
        probe_value_offset = 9

        # create the BenchmarkConfig object
        config = BenchmarkConfig(selectivity, build_to_probe_ratio, duplicates, probe_cardinality)
        self.assertEqual(config.probe_size, probe_size)
        self.assertEqual(config.build_size, build_size)
        self.assertEqual(config.value_range, value_range)
        self.assertEqual(config.probe_value_offset, probe_value_offset)

    def test_combined(self):
        selectivity = 0.1
        build_to_probe_ratio = 4.0
        duplicates = 5
        probe_cardinality = 1000

        # expected values
        probe_size = 1000
        build_size = 250
        value_range = 50
        probe_value_offset = 45

        # create the BenchmarkConfig object
        config = BenchmarkConfig(selectivity, build_to_probe_ratio, duplicates, probe_cardinality)
        self.assertEqual(config.probe_size, probe_size)
        self.assertEqual(config.build_size, build_size)
        self.assertEqual(config.value_range, value_range)
        self.assertEqual(config.probe_value_offset, probe_value_offset)






def get_join_micro_probe_benchmark() -> Benchmark:

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

    n_total_queries = len(JOIN_PROBE_SELECTIVITIES) * len(JOIN_BUILD_TO_PROBE_RATIOS) * len(JOIN_BUILD_DUPS)
    n_created_queries = 0

    # set the seed for reproducibility
    con = duckdb.connect(duckdb_file_path)
    con.execute(f"SELECT setseed(0.42);")
    for selectivity in JOIN_PROBE_SELECTIVITIES:
        for build_to_probe_ratio in JOIN_BUILD_TO_PROBE_RATIOS:
            for duplicates in JOIN_BUILD_DUPS:
                selectivity_int = round(100 * selectivity)
                build_to_probe_ratio_int = round(build_to_probe_ratio)
                config = BenchmarkConfig(selectivity, build_to_probe_ratio, duplicates, JOIN_PROBE_CARDINALITY)
                (p_name, b_name), (p_query, b_query) = config.get_queries()

                expected_cardinality = config.expected_cardinality
                if expected_cardinality > 1_000_000_000:
                    logger.warning(f"Expected result cardinality is too high: {expected_cardinality}. Skipping query.")
                    continue

                query = {
                    'name': f'join_micro_probe_{build_to_probe_ratio_int}_{selectivity_int}_{duplicates}',
                    'index': len(queries),
                    'run_script': {
                        "duckdb": f"SELECT * FROM {p_name} as probe JOIN {b_name} as build ON probe.key = build.key;"
                    },
                    'config': {
                        'build_to_probe_ratio': build_to_probe_ratio,
                        'selectivity': selectivity,
                        'duplicates': duplicates,
                        'probe_cardinality': JOIN_PROBE_CARDINALITY,
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
        'name': 'join_micro_probe',
        'datasets': datasets,
        'queries': queries
    }


def __get_file_path() -> str:
    file_name =  os.path.join('join', f'micro_probe.db')
    return get_data_path(file_name)


def __generate_and_return_data() -> List[DataSet]:

    duckdb_file_path = __get_file_path()
    duckdb_file_name_without_extension = os.path.splitext(os.path.basename(duckdb_file_path))[0]


    setup_script = {
        'duckdb': f"ATTACH '{duckdb_file_path}' (READ_ONLY); USE '{duckdb_file_name_without_extension}'; PRAGMA disable_optimizer; PRAGMA disable_progress_bar;"
    }

    dataset: DataSet = {
        'name': f'join-micro-probe',
        'setup_script': setup_script,
        'config': {}
    }

    return [dataset]


if __name__ == "__main__":
    # This is just for testing the data generation
    get_join_micro_probe_benchmark()
    logger.info("Data generation finished.")