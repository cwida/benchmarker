import os
import sys

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from typing import TypedDict, Literal, Callable, List, Dict, Optional, Tuple, Union

SystemName = Literal['duckdb']


class EnvironmentVariable(TypedDict):
    name: str
    value: str


class Script(TypedDict):
    duckdb: str


class AdaptiveParameters(TypedDict):
    n_runs: int
    n_chains: int
    timestamp: int
    join_index: int


class SystemSourceCodeLocation(TypedDict):
    location: Literal['github', 'local']
    github_url: Optional[str] # can be to a repo, a branch or a commit
    local_path: Optional[str]

class SystemBuildConfig(TypedDict):
    location: SystemSourceCodeLocation
    build_command: str

class SystemRunConfig(TypedDict):
    run_file: str
    run_file_relative_to_build: bool

# takes the thread index as argument, gets the command as a string
GetStartProfilerCommand = Callable[[int], str]

# callback function that returns a runtime (float) and cardinality (int), gets thread index as argument
GetMetricsFunction = Callable[[int], Optional[Tuple[float, int]]]


class System(TypedDict):
    name: SystemName
    version: str
    build_config: Optional[SystemBuildConfig]
    run_config: SystemRunConfig
    setup_script: str
    set_threads_command: Callable[[int], str]  # takes the number of threads as argument
    get_start_profiler_command: GetStartProfilerCommand
    get_metrics: GetMetricsFunction  # callback function that returns a float, gets thread index as argument


class DataSet(TypedDict):
    name: str
    setup_script: Script
    config: Dict[str, any]


class Query(TypedDict):
    name: str
    index: int
    run_script: Script
    config: Dict[str, any] | None

AWSEdgeTypes = Literal['generated', 'snap', 'all']
ExperimentMode = Literal['train', 'test', 'all']
DataSource = Literal['tables', 'parquet']
SchemaType = Literal['without', 'along']


class RunSettings(TypedDict, total=False):
    seed: Optional[float]
    n_parallel: Optional[int]
    n_runs: Optional[int]
    timeout: Optional[int]

    offset: Optional[int]
    max_n_experiments: Optional[int]

class RunSettingsInternal(TypedDict):
    seed: float
    n_parallel: int
    n_runs: int
    timeout: int

    offset: int
    max_n_experiments: Optional[int]

def run_settings_fill_defaults(settings: RunSettings) -> RunSettingsInternal:
    return {
        'seed': 0.42,
        'n_parallel': 1,
        'n_runs': 5,
        'timeout': 60,
        'offset': 0,
        'max_n_experiments': None,
        **settings
    }

class SystemSettings(TypedDict):
    n_threads: int

# All queries will be run on all datasets
class Benchmark(TypedDict):
    name: str
    datasets: List[DataSet]
    queries: List[Query]

class RunConfig(TypedDict, total=False):
    name: str
    run_settings: RunSettings

    # we can test over multiple system settings
    system_settings: Union[SystemSettings, List[SystemSettings]]
    systems: Union[System, List[System]]
    benchmarks: Union[Benchmark, List[Benchmark]]

# consists of one system with one setting and one benchmark
class Experiment(TypedDict):
    name: str
    run_name: str
    run_date: str
    data: DataSet
    settings: RunSettingsInternal
    query: Query
    system_setting: SystemSettings
    system: System


class ExperimentResult(TypedDict):
    runtimes: List[float]
    cardinalities: List[int]
    experiment: Experiment
