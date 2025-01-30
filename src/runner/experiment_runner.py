import json
import logging
import os
import sys
from asyncio import sleep
from queue import Queue
from threading import Lock, Thread

from tqdm import tqdm

from src.builder.system_builder import build_systems, get_system_identifier, Status, \
    run_script
from src.models import RunConfig, Experiment, RunSettingsInternal, System, ExperimentResult, DataSet, Query
from src.runner.experiment_prepper import create_experiments_from_config, get_empty_result, get_experiment_script
from src.utils import get_experiment_output_path_json, SafeEncoder

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from typing import List


def run(config: RunConfig):
    experiments, run_settings = create_experiments_from_config(config)
    build_systems(config['systems'])

    run_experiments(experiments, run_settings)


def run_experiments(experiments: List[Experiment], settings: RunSettingsInternal):
    n_parallel = settings['n_parallel']

    if n_parallel > 1:
        logging.info(f"Running experiments in parallel with {n_parallel} threads...")
        run_experiment_parallel(experiments, settings)
    else:
        logging.info("Running experiments sequentially...")
        run_experiment_sequential(experiments)


def run_experiment_parallel(experiments: List[Experiment], settings: RunSettingsInternal):
    n_parallel = settings['n_parallel']
    experiment_queue = Queue()
    progress_lock = Lock()  # Lock for updating the tqdm bar safely
    progress_bar = tqdm(total=len(experiments), desc="Running experiments", position=0, leave=True)

    # Populate the queue with experiments
    for experiment in experiments:
        experiment_queue.put(experiment)

    def worker(thread_index: int):
        while not experiment_queue.empty():
            try:
                experiment = experiment_queue.get_nowait()  # Get an experiment from the queue
                run_experiment(experiment, thread_index)
                experiment_queue.task_done()

                # Update the progress bar safely
                with progress_lock:
                    progress_bar.update(1)

            except Queue.Empty:
                break  # Queue is empty, exit the loop

    # Start threads to run experiments
    threads = []
    for i in range(n_parallel):
        thread = Thread(target=worker, args=(i,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Close the progress bar after all experiments are done
    progress_bar.close()


def run_experiment_sequential(experiments: List[Experiment]):
    for (index, experiment) in enumerate(experiments):
        run_experiment(experiment, 0)

def run_experiment(experiment: Experiment, thread_index: int):
    experiment_result: ExperimentResult = get_empty_result(experiment)

    settings = experiment['settings']
    system: System = experiment['system']
    system_settings = experiment['system_setting']

    system_identifier = get_system_identifier(system)
    timeout = settings['timeout']

    data: DataSet = experiment['data']
    query: Query = experiment['query']
    n_runs: int = settings['n_runs']

    env_vars = {}

    script = get_experiment_script(system, data, query, system_settings, thread_index)

    runtimes = []
    cardinalities = []

    for i in range(n_runs):

        status: Status = run_script(system, script, timeout, thread_index, env_vars)

        if status != 'success':
            logging.error(f"Error in running {system['name']}-{system['version']}")
            sleep(0.2)  # wait for process to finish to release db locks
            if status == 'crash' or status == 'timeout':  # if timeout or crash, break
                break

        metrics_retrieved = system['get_metrics'](thread_index)
        if metrics_retrieved is None:
            logging.error(f"Error in retrieving metrics for {system['name']}-{system['version']}: {metrics_retrieved}")
            break
        else:
            duration, result_cardinality = metrics_retrieved

        runtimes.append(duration)
        cardinalities.append(result_cardinality)

    # save the results as a json file
    path = get_experiment_output_path_json(experiment)
    with open(path, 'w') as f:
        json.dump(experiment_result, f, indent=4, cls=SafeEncoder)
