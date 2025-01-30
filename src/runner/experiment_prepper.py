import logging
import os
import sys
from typing import List, Tuple

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from src.models import RunConfig, Experiment, run_settings_fill_defaults, Benchmark, System, SystemSettings, \
    RunSettingsInternal, ExperimentResult, DataSet, Query



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
    logging.info(f"Number of cores required: {cores_required}")

    number_of_cores = os.cpu_count()
    logging.info(f"Number of cores available: {number_of_cores}")

    if cores_required > number_of_cores:
        logging.warning(f"Number of cores required ({cores_required}) is greater than the number of cores available ({number_of_cores})")

    index = 0
    for benchmark in benchmarks:
        for system in systems:
            for system_setting in system_settings:
                for query in benchmark['queries']:
                    for data in benchmark['datasets']:
                        name = benchmark['name'] + '-experiment-' + str(index)
                        experiment: Experiment = {
                            'name': name,
                            'run_name': config['name'],
                            'data': data,
                            'settings': run_settings,
                            'query': query,
                            'system_setting': system_setting,
                            'system': system
                        }
                        experiments.append(experiment)

    offset = run_settings['offset']
    if offset is None:
        offset = 0
    max_n_experiments = run_settings['max_n_experiments']
    if max_n_experiments is not None:
        experiments = experiments[offset:offset + max_n_experiments]
    else:
        experiments = experiments[offset:]

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