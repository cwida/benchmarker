import os
import sys

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from config.benchmark.tpcds import get_tpcds_benchmark
from config.benchmark.tpch import get_tpch_benchmark
from config.systems.duckdb import DUCK_DB_NIGHTLY, DUCK_DB_WITHOUT_ATOMICS, DUCK_DB_JOIN_OPTIMIZATION_BASELINE, \
    DUCK_DB_JOIN_OPTIMIZATION_HASH_MARKER_AND_COLLISION_BIT, DUCK_DB_JOIN_OPTIMIZATION_HASH_MARKER
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [30]
    config: RunConfig = {
        'name': 'duckdb_collision_bits',
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 4,
        },
        'system_settings': [
            {'n_threads': 6},
        ],
        'systems': [DUCK_DB_JOIN_OPTIMIZATION_BASELINE, DUCK_DB_JOIN_OPTIMIZATION_HASH_MARKER, DUCK_DB_JOIN_OPTIMIZATION_HASH_MARKER_AND_COLLISION_BIT],
        'benchmarks': get_tpch_benchmark(sfs),
    }
    run(config)

if __name__ == "__main__":
    main()