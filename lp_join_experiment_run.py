from config.benchmark.tpch import get_tpch_benchmark
from config.systems.duckdb import DUCK_DB_LP_JOIN_NO_SALT, DUCK_DB_LP_JOIN_BASELINE, DUCK_DB_LP_JOIN
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [1, 3, 10]
    config: RunConfig = {
        'name': 'lp_join_experiment',
        'run_settings': {
            'n_parallel': 2,
        },
        'system_settings': [
            {'n_threads': 1},
            {'n_threads': 2},
            # {'n_threads': 4},
            # {'n_threads': 8},
        ],
        'systems': [DUCK_DB_LP_JOIN_NO_SALT, DUCK_DB_LP_JOIN_BASELINE, DUCK_DB_LP_JOIN],
        'benchmarks': get_tpch_benchmark(sfs),
    }
    run(config)


if __name__ == "__main__":
    main()