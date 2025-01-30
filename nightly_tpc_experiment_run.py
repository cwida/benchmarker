from typing import List, Literal

from config.models import DataSet, RunConfig
from config.query import TPCH_QUERIES, TPCDS_QUERIES
from config.system import DUCK_DB_V113
from src.utils_data import generate_and_return_tpc_data
from src.utils_experiments import create_experiments_for_datasets
from src.utils_run import run_experiments
from utils import EXPERIMENT_BASE_NAME_TPCH

Benchmark = Literal['tpch', 'tpcds']

def get_tpc_data(sfs: List[int], benchmark: Benchmark, verbose: bool = False) -> List[DataSet]:
    return generate_and_return_tpc_data(sfs, benchmark, verbose)

def main():
    sfs = [1, 3, 10, 30]

    systems = [DUCK_DB_V113]
    benchmark: Benchmark = 'tpch'
    settings: RunConfig = {
        'n_parallel': 1,  # the number of experiments to run in parallel
        'n_runs': 5,  # the number of times a query is run
        'n_threads': 4,  # the number of threads to use
        'timeout': 120,  # the timeout for a query in seconds

        'max_n_experiments': None,  # the maximum number of experiments to run, None for all
        'verbose': True,  # whether to print verbose output
        'base_name': EXPERIMENT_BASE_NAME_TPCH,  # the base name for the experiments
        'mode': 'train',
        'offset': 0,  # the offset for the experiments,
        'gather_adaptive_params': False,
        'seed': 0.42,
    }

    datasets = get_tpc_data(sfs, benchmark, settings['verbose'])
    queries = TPCH_QUERIES if benchmark == 'tpch' else TPCDS_QUERIES
    experiments = create_experiments_for_datasets(datasets, queries, systems, settings)

    run_experiments(experiments, settings)


if __name__ == "__main__":
    main()

"""
WITH data AS (
    SELECT
        list_avg(list_transform(results."duckdb-v1.1.3", s -> s.runtime)) as d_runtime,
        CAST(replace(experiment.data.name, 'tpch-', '') AS INT) as sf,
        experiment.settings.n_threads as threads
    FROM 'output/result/tpch/json/*.json'
),
single_thread AS (
    SELECT
        sf,
        round(geomean(d_runtime), 3) as single_thread_runtime
    FROM data
    WHERE threads = 1
    GROUP BY sf
)
SELECT
    threads,
    sf,
    round(geomean(d_runtime), 3) as runtime,
    single_thread_runtime,
    round((single_thread_runtime / runtime) * (1 / threads), 3) as parallel_efficiency
FROM data
JOIN single_thread USING (sf)
GROUP BY threads, sf, single_thread_runtime
ORDER BY threads, sf;
"""