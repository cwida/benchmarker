# Benchmarker

This is a simple benchmarking tool for SQL-based systems.

## Experiment Configuration

You can create an experiment by adding a new python_script in the `experiments` directory. For an easy start,
just copy an existing experiment and adjust the parameters. All output will be stored in the
`_output/results/<experiment_name>/<timestamp>/` directory,
with the raw run data in the `output/runs/<experiment_name>/<timestamp>/` directory and the summary in the

Example:

```python
from config.benchmark.tpcds import get_tpcds_benchmark
from config.systems.duckdb import DUCK_DB_V113, DUCK_DB_NIGHTLY
from src.models import RunConfig
from src.runner.experiment_runner import run


def main():
    sfs = [1, 3]
    config: RunConfig = {
        'name': 'tpcds_nightly_experiment',
        'run_settings': {
            'n_parallel': 1,
            'n_runs': 3,
        },
        'system_settings': [
            {'n_threads': 1},
            {'n_threads': 2},
            {'n_threads': 4},
            {'n_threads': 8},
        ],
        'systems': [DUCK_DB_V113, DUCK_DB_NIGHTLY],
        'benchmarks': get_tpcds_benchmark(sfs),
    }
    run(config)


if __name__ == "__main__":
    main()
```

Each experiment should have a main() that calls the run() function with the experiment configuration. The configuration
is structured as follows:

1. `name`: The name of the experiment. This is used to create a directory in the `_output/results` directory.
   Additionally,
   there will also be one folder per run in this directory so you can run the same experiment multiple times and compare
   the results.
2. `run_settings`: The settings for the experiment run:
    1. `n_parallel`: The number of parallel runs for running multiple runs in parallel to speed up
       the experiment. Make sure that your number of cores is not exceeded. Defaults to `1`.
    2. `n_runs`: The number of runs per system setting. This is useful if you want to run multiple runs to get a more
       stable result. Defaults to `5`.
    3. `seed`: The seed for the random number generator during data generation. Defaults to `0.42`.
    4. `timeout`: The timeout for each run in seconds. Defaults to `60`.
    5. `offset`: The offset for the run number if you want to continue an experiment that was interrupted.
       Defaults to `0`.
    6. `max_n_experiments`: The maximum number of experiments to run. Defaults to `None`.
3. `system_settings`: Can be a single dictionary or a list of dictionaries. If multiple dictionaries are provided, the
   experiment will be run for each system setting. Each dictionary should contain the system setting parameters:
    1. `n_threads`: The number of threads to use for the system. Defaults to `1`.
4. `systems`: A list of system configurations to run the experiment on. See more
   at [System Configuration](#system-configuration).
5. `benchmarks`: A list of benchmarks to run. See more at [Benchmark Configuration](#benchmark-configuration).

## System Configuration

You can configure the systems in the `config/systems` directory. Each system should have a python file with a
dictionary. For DuckDB, there is already a configuration in `config/systems/duckdb.py`:

```python
DUCK_DB_MAIN: System = {
    'version': 'v1.0.0',
    'name': 'duckdb',
    'build_config': {
        'build_command': 'GEN=ninja BUILD_HTTPFS=1 BUILD_TPCH=1 BUILD_TPCDS=1 make',
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/duckdb/duckdb/commit/1f98600c2cf8722a6d2f2d805bb4af5e701319fc',
        },
    },
    'run_config': {
        'run_file': 'build/release/duckdb <',
        'run_file_relative_to_build': True,
    },
    'setup_script': '',
    'set_threads_command': lambda n_threads: f"PRAGMA threads = {n_threads};",
    'get_start_profiler_command': get_duckdb_profile_script,
    'get_metrics': get_duckdb_runtime_and_cardinality,
}
```

The `name` and `version` are used for identifying the system in the output.

### Build Configuration

To build a system from source, you can pass a `build_command` together with the `location` of the source code.
The location can either be a `github` repository or a `local` directory.

**GitHub Repository:**

- `location`: Must be set to `github`.
- `github_url`: The URL of the github repository. Can be a link to a) a specific commit, b) a branch, or c) the root of
  the repository.

**Local Directory:**

- `location`: Must be set to `local`.
- `local_path`: The path to the local directory.

At the start of the experiment, the system will be cloned into the `_output/systems` directory. The build command will
be executed in the cloned directory.
Also, when on a branch or repository, the newest commit will be fetched. So one workflow could be to push a new commit
to the repository and then re-run the experiment to test the new changes.

### Run Configuration

The `run_config` contains the command to run the system. The `run_file` is the command to run the system.
If the command is relative to the build directory, set `run_file_relative_to_build` to `True`. This is necessary e.g.
when building from GitHub.

The run command *must* be able to take a SQL file as input. This is why for DuckDB, the command is
`build/release/duckdb <`. This will be then used as `build/release/duckdb < <SQL_FILE>`.

### Setup Script

Can be used to set up the system before running the benchmarks. Not necessary for DuckDB. But here e.g. one could
install a DuckDB community extension.

### Set Threads Command

This callback function is used to set the number of threads for the system. For DuckDB, this is done by setting the
`PRAGMA threads` command.

### Get Start Profiler Command

This callback function is used to start the profiler for the system. As experiments can be run in parallel, the profiler
takes the `thread_index` as an argument so you can distinguish between the different threads. For DuckDB, this looks
like:

```python
def get_duckdb_profile_script(thread: int) -> str:
    path = get_profile_path_duckdb(thread)
    string = f"PRAGMA enable_profiling = 'json';pragma profile_output='{path}';"
    return string
```

### Get Metrics

Here we need to return the metrics per query. We collect the cardinality for consistency checks and the runtime.
For DuckDB, this is a bit complicated as we need to parse the JSON output of the profiler we just configured above.

```python
def get_duckdb_runtime_and_cardinality(thread: int) -> Optional[Tuple[float, int]]:
    # load the json
    json_path = get_profile_path_duckdb(thread)

    # if the query crashed, the file does not exist
    if not os.path.exists(json_path):
        return None

    with open(json_path, 'r') as f:
        profile = json.load(f)

    # get the runtime, can be either in the timing or operator_timing field because of different DuckDB versions
    if 'timing' in profile:
        runtime = profile['timing']
        cardinality = profile['children'][0]['children'][0]['cardinality']
    elif 'operator_timing' in profile:
        runtime = profile['operator_timing']
        try:
            cardinality = profile['children'][0]['cardinality']
        except KeyError:  # Sometimes operator cardinality is not found for whatever reason
            cardinality = profile['children'][0]['operator_cardinality']

    elif 'latency' in profile:
        runtime = profile['latency']
        cardinality = profile['result_set_size']

    else:
        # throw an error if the runtime is not found
        raise ValueError(f'Runtime not found in profile: {profile}')
    # delete the file
    os.remove(json_path)
    return runtime, cardinality
```

## Benchmark Configuration

The Benchmarks consist of a list of queries and a list of datasets.

```python
class Benchmark(TypedDict):
    name: str
    datasets: List[DataSet]
    queries: List[Query]

class DataSet(TypedDict):
    name: str
    setup_script: Script
    config: Dict[str, any]

class Query(TypedDict):
    name: str
    index: int
    run_script: Script
 ```
For each benchmark, *each `Query` will be run on each `DataSet`*. Therefore, the must be compatible (all the tables that
the query needs must be present in the dataset).

For `TPC-DS` and `TPC-H`, there are already configurations in `config/benchmark/tpcds.py` and `config/benchmark/tpch.py` for 
DuckDB.

### Example: TPC-H
```python
TPC_H_QUERIES: List[Query] = [
    {
        'name': f'tpch{i + 1}',
        'index': i,
        'run_script': {
            "duckdb": f"PRAGMA tpch({i + 1});",
        }
    } for i in range(22)
]
```
Running the TPC-H queries is as simple as setting the `run_script` to the `PRAGMA` command that runs the query. But 
if you would like to run a custom query, you have to see the `duckdb` implementation of the run script:
```python
'run_script': {
    "duckdb": f"SELECT * FROM WHATEVER WHERE SOMETHING = {i + 1};",
}
```
For `TPC-H`, the data is also generated and stored under `output/data/tpch/`. To generate data for your custom query,
look at the tpch.py file in the `config/benchmark` directory and adjust it for your needs.