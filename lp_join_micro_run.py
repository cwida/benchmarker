from typing import List, Literal

import numpy as np
from config.models import RunConfig
from config.system import DUCK_DB_LP_JOIN, DUCK_DB_LP_JOIN_BASELINE
from src.micro_join import generate_micro_benchmark_data, get_micro_query
from src.utils_experiments import create_experiments_for_datasets
from src.utils_run import run_experiments
from utils import EXPERIMENT_BASE_NAME_MICRO

Benchmark = Literal['tpch', 'tpcds']

def get_data_size(rows) -> float:

    print(f"Rows: {rows}")
    ROWS_SIZE_BYTES = 8  # 8 bytes per row

    bytes = rows * ROWS_SIZE_BYTES
    gigabytes = bytes / 1_000_000_000
    gigabytes = round(gigabytes, 4)
    return gigabytes


def generate_log_steps(max_value, num_steps) -> List[int]:
    """
    Generates logarithmically spaced steps between 1 and a specified maximum value.

    Parameters:
    - max_value (float): The maximum value of the range.
    - num_steps (int): The number of logarithmic steps to generate.

    Returns:
    - numpy.ndarray: An array of logarithmically spaced steps from 1 to max_value.
    """
    # Using numpy to generate logarithmically spaced steps
    float_list = list(np.logspace(0, np.log10(max_value), num_steps))
    return [int(round(value)) for value in float_list]

def generate_linear_steps(max_value, num_steps) -> List[int]:
    """
    Generates linearly spaced steps between 1 and a specified maximum value.

    Parameters:
    - max_value (float): The maximum value of the range.
    - num_steps (int): The number of linear steps to generate.

    Returns:
    - numpy.ndarray: An array of linearly spaced steps from 1 to max_value.
    """
    # Using numpy to generate linearly spaced steps
    float_list = list(np.linspace(1, max_value, num_steps, dtype=int))
    return [int(round(value)) for value in float_list]

def main():

    CARDINALITY = 286_435_456 // 64
    CARDINALITY = 1_000_000

    N_DUPLICATES = generate_log_steps(100, 10)
    N_SELECTIVITIES = generate_log_steps(100, 1)
    # transfer selectivities to percentages
    max_data_size = get_data_size(CARDINALITY)
    print(f"Max data size: {max_data_size} GB")

    systems = [DUCK_DB_LP_JOIN_BASELINE, DUCK_DB_LP_JOIN]
    benchmark: Benchmark = 'tpch'
    settings: RunConfig = {
        'seed': 0.42,  # the seed for the experiments
        'n_parallel': 1,  # the number of experiments to run in parallel
        'n_runs': 5,  # the number of times a query is run
        'n_threads': 2,  # the number of threads to use
        'timeout': 30,  # the timeout for a query in seconds

        'max_n_experiments': None,  # the maximum number of experiments to run, None for all
        'verbose': False,  # whether to print verbose output
        'base_name': EXPERIMENT_BASE_NAME_MICRO,  # the base name for the experiments
        'mode': 'train',
        'gather_adaptive_params': False,
        'offset': 0  # the offset for the experiments
    }

    datasets = generate_micro_benchmark_data(CARDINALITY, N_SELECTIVITIES, N_DUPLICATES, settings['verbose'], settings['seed'])
    queries = [get_micro_query()]
    experiments = create_experiments_for_datasets(datasets, queries, systems, settings)

    # reverse experiments
    experiments = experiments[::-1]
    run_experiments(experiments, settings)


if __name__ == "__main__":
    main()
