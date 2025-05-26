import os
import sys

from config.benchmark.clickbench import get_clickbench_benchmark

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from config.systems.duckdb import DUCK_DB_STRING_EQUALITY_BASELINE, DUCK_DB_STRING_EQUALITY_V1
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [30]
    # file name without extension
    default_name =  os.path.splitext(os.path.basename(__file__))[0]
    config: RunConfig = {
        'name': default_name,
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 4,
        },
        'system_settings': [
            {'n_threads': 4},
        ],
        'systems': [DUCK_DB_STRING_EQUALITY_BASELINE, DUCK_DB_STRING_EQUALITY_V1],
        'benchmarks': get_clickbench_benchmark(),
    }
    run(config)

if __name__ == "__main__":
    main()