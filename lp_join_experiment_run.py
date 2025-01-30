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
        'logging_level': 'INFO',
    }

    run(config)


if __name__ == "__main__":
    main()

"""
        'n_parallel': 1,  # the number of experiments to run in parallel
        'n_runs': 5,  # the number of times a query is run
        'n_threads': 1,  # the number of threads to use
        'timeout': 120,  # the timeout for a query in seconds

        'max_n_experiments': None,  # the maximum number of experiments to run, None for all
        'verbose': False,  # whether to print verbose output
        'base_name': EXPERIMENT_BASE_NAME_LP_JOIN,  # the base name for the experiments
        'mode': 'train',
        'offset': 0,  # the offset for the experiments,
        'gather_adaptive_params': False,
        'seed': 0.42,
"""
