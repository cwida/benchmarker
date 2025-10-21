import os
import sys



root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from config.benchmark.tpch import get_tpch_benchmark
from config.benchmark.imdb import get_imdb_benchmark
from config.benchmark.tpcds import get_tpcds_benchmark

from config.systems.duckdb import DUCK_DB_BF_BASELINE, DUCK_DB_BF_V1, DUCK_DB_BF_X86, DUCK_DB_EARLY_PROBING
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [10]
    config: RunConfig = {
        'name': str(os.path.basename(__file__)).split('.')[0],
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 5,
        },
        'system_settings': [
            # {'n_threads': 1},
            # {'n_threads': 4},
            {'n_threads': 8},
            # {'n_threads': 10},
        ],
        'systems': [DUCK_DB_BF_V1,  DUCK_DB_BF_X86, DUCK_DB_BF_BASELINE],
        'benchmarks': [get_imdb_benchmark()] ,
    }
    run(config)

if __name__ == "__main__":
    main()