import os
import sys

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)


from config.benchmark.tpch import get_tpch_benchmark
from config.systems.duckdb import DUCK_DB_PARTITIONED, DUCK_DB_NIGHTLY_BUILD_LOCALLY
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [30]
    config: RunConfig = {
        'name': 'duckdb_partitioned_ht_experiment',
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 4,
        },
        'system_settings': [
            {'n_threads': 1},
            {'n_threads': 2},
            {'n_threads': 10},
        ],
        'systems': [DUCK_DB_NIGHTLY_BUILD_LOCALLY, DUCK_DB_PARTITIONED],
        'benchmarks': get_tpch_benchmark(sfs),
    }
    run(config)

if __name__ == "__main__":
    main()