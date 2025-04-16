import os
import sys

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from config.systems.duckdb import DUCK_DB_NIGHTLY_BUILD_LOCALLY, DUCK_DB_PARTITIONED_NO_ATOMICS, \
    DUCK_DB_PARTITIONED_WITH_ATOMICS
from config.benchmark.join_micro_build import get_join_micro_build_benchmark

from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    config: RunConfig = {
        'name': str(os.path.basename(__file__)).split('.')[0],
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 5,
        },
        'system_settings': [
            {'n_threads': 1},
            {'n_threads': 2},
            {'n_threads': 4},
            {'n_threads': 8},
        ],
        'systems': [DUCK_DB_NIGHTLY_BUILD_LOCALLY, DUCK_DB_PARTITIONED_NO_ATOMICS, DUCK_DB_PARTITIONED_WITH_ATOMICS],
        'benchmarks': get_join_micro_build_benchmark(),
    }

    run(config)


if __name__ == "__main__":
    main()
