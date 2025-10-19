import os
import sys



root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)
from config.benchmark.clickbench import get_clickbench_benchmark
from config.benchmark.tpch import get_tpch_benchmark

from config.systems.duckdb import DUCK_DB_ROW_MATCHER_OPTIONAL_NULL, DUCK_DB_ROW_MATCHER_BASELINE
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
            {'n_threads': 8},
        ],
        'systems': [DUCK_DB_ROW_MATCHER_OPTIONAL_NULL, DUCK_DB_ROW_MATCHER_BASELINE],
        'benchmarks': get_clickbench_benchmark(),
    }
    run(config)

if __name__ == "__main__":
    main()