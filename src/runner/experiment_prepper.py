from datetime import datetime
import os
import sys
from typing import List, Tuple

from src.logger import get_logger

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from src.models import RunConfig, Experiment, run_settings_fill_defaults, Benchmark, System, SystemSettings, \
    RunSettingsInternal, ExperimentResult, DataSet, Query

logger = get_logger(__name__)

def create_experiments_from_config(
        config: RunConfig,
) -> Tuple[List[Experiment], RunSettingsInternal]:
    experiments = []

    # if benchmarks is a list of benchmarks, use that, else create a list of one benchmark
    benchmarks: List[Benchmark] = config['benchmarks'] if isinstance(config['benchmarks'], list) else [config['benchmarks']]
    systems: List[System] = config['systems'] if isinstance(config['systems'], list) else [config['systems']]
    system_settings: List[SystemSettings] = config['system_settings'] if isinstance(config['system_settings'], list) else [config['system_settings']]
    run_settings = run_settings_fill_defaults(config['run_settings'])

    max_threads = max([s['n_threads'] for s in system_settings])

    cores_required = run_settings['n_parallel'] * max_threads
    logger.info(f"Number of cores required: {cores_required}")

    number_of_cores = os.cpu_count()
    logger.info(f"Number of cores available: {number_of_cores}")

    if cores_required > number_of_cores:
        logger.warning(f"Number of cores required ({cores_required}) is greater than the number of cores available ({number_of_cores})")
    else:
        logger.info(f"Number of cores required ({cores_required}) is less than the number of cores available ({number_of_cores})")
    index = 0

    # get the current run date and time so we can use it  as folder name
    run_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    for benchmark in benchmarks:
        for query in benchmark['queries']:
            for data in benchmark['datasets']:
                for system in systems:
                    for system_setting in system_settings:
                        name = benchmark['name'] + '-experiment-' + str(index)
                        experiment: Experiment = {
                            'name': name,
                            'run_name': config['name'],
                            'run_date': run_date,
                            'data': data,
                            'settings': run_settings,
                            'query': query,
                            'system_setting': system_setting,
                            'system': system
                        }
                        experiments.append(experiment)
                        index += 1

    offset = run_settings['offset']
    if offset is None:
        offset = 0
    max_n_experiments = run_settings['max_n_experiments']
    if max_n_experiments is not None:
        experiments = experiments[offset:offset + max_n_experiments]
    else:
        experiments = experiments[offset:]

    logger.info(f"Created {len(experiments)} experiments from {len(benchmarks)} benchmarks, {len(systems)} systems, and {len(system_settings)} system settings")

    return experiments, run_settings


def get_experiment_script(system: System, data: DataSet, query: Query, settings: SystemSettings, run_thread_index: int) -> str:
    script: str = ''
    system_name = system['name']
    system_threads = settings['n_threads']
    script += system['setup_script'] + '\n'
    script += data['setup_script'][system_name] + '\n'

    script += system['get_start_profiler_command'](run_thread_index) + '\n'
    script += system['set_threads_command'](system_threads) + '\n'

    script += query['run_script'][system_name] + '\n'

    return script



def get_empty_result(experiment: Experiment) -> ExperimentResult:

    return {
        'experiment': experiment,
        'runtimes': [],
        'cardinalities': []
    }