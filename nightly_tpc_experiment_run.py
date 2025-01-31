from config.benchmark.tpch import get_tpch_benchmark
from config.systems.duckdb import DUCK_DB_V113, DUCK_DB_NIGHTLY
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [1, 3, 10]
    config: RunConfig = {
        'name': 'tpch_nightly_experiment',
        'run_settings': {
            'n_parallel': 2,
        },
        'system_settings': [
            {'n_threads': 1},
            {'n_threads': 2},
            # {'n_threads': 4},
            # {'n_threads': 8},
        ],
        'systems': [DUCK_DB_V113, DUCK_DB_NIGHTLY],
        'benchmarks': get_tpch_benchmark(sfs),
    }
    run(config)

if __name__ == "__main__":
    main()