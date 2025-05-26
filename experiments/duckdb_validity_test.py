import os
import sys

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)


from config.benchmark.tpcds import get_tpcds_benchmark
from config.systems.duckdb import DUCK_DB_VALIDITY_DISABLED, DUCK_DB_VALIDITY_BASELINE, DUCK_DB_VALIDITY_V1
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [10]
    config: RunConfig = {
        'name': 'validity_experiment',
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 5,
        },
        'system_settings': [
            {'n_threads': 4},
        ],
        'systems': [DUCK_DB_VALIDITY_BASELINE, DUCK_DB_VALIDITY_V1, DUCK_DB_VALIDITY_DISABLED],
        'benchmarks': get_tpcds_benchmark(sfs),
    }
    run(config)

if __name__ == "__main__":
    main()