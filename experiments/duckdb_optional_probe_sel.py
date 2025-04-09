import os
import sys


root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from config.benchmark.imdb import get_imdb_benchmark

from config.benchmark.tpch import get_tpch_benchmark
from config.systems.duckdb import DUCK_DB_NIGHTLY_BUILD_LOCALLY, DUCK_DB_OPTIONAL_PROBE_SEL
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [30]
    config: RunConfig = {
        'name': 'duckdb_optional_probe_sel_experiment',
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 4,

        },
        'system_settings': [
            {'n_threads': 4},
        ],

        'systems': [DUCK_DB_NIGHTLY_BUILD_LOCALLY, DUCK_DB_OPTIONAL_PROBE_SEL],
        'benchmarks': [get_tpch_benchmark([10, 30])],
    }
    run(config)

if __name__ == "__main__":
    main()