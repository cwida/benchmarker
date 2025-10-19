import json
import os
import sys
from asyncio import sleep
from queue import Queue
from threading import Lock, Thread

from tqdm import tqdm

from src.builder.system_builder import build_systems, get_system_identifier, Status, \
    run_script
from src.eval.run_evaluation import run_evaluation
from src.logger import get_logger
from src.models import RunConfig, Experiment, RunSettingsInternal, System, ExperimentResult, DataSet, Query
from src.runner.experiment_prepper import create_experiments_from_config, get_empty_result, get_experiment_script
from src.utils import get_experiment_output_path_json, SafeEncoder

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from typing import List
logger = get_logger(__name__)

def run(config: RunConfig):

    experiments, run_settings = create_experiments_from_config(config)
    build_systems(config['systems'])

    run_experiments(experiments, run_settings)

    run_evaluation(config['name'])


def run_experiments(experiments: List[Experiment], settings: RunSettingsInternal):
    n_parallel = settings['n_parallel']

    if n_parallel > 1:
        logger.info(f"Running experiments in parallel with {n_parallel} threads...")
        run_experiment_parallel(experiments, settings)
    else:
        logger.info("Running experiments sequentially...")
        run_experiment_sequential(experiments)


def run_experiment_parallel(experiments: List[Experiment], settings: RunSettingsInternal):
    n_parallel = settings['n_parallel']
    experiment_queue = Queue()
    progress_lock = Lock()  # Lock for updating the tqdm bar safely

    progress_bar = tqdm(
        total=len(experiments),
        desc="Running experiments",
        leave=True,
        dynamic_ncols=True,
        file=sys.stdout,  # Sometimes helps in certain environments
    )

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

            except Exception as e:
                logger.error(f"Error in thread {thread_index}: {e}")
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
    for (index, experiment) in tqdm(enumerate(experiments), desc="Running experiments", position=0, leave=True):
        run_experiment(experiment, 0)

def run_experiment(experiment: Experiment, thread_index: int):
    experiment_result: ExperimentResult = get_empty_result(experiment)

    logger.info(f"Running experiment {experiment['name']} with system {experiment['system']['name']}-{experiment['system']['version']}")
    settings = experiment['settings']
    system: System = experiment['system']
    system_settings = experiment['system_setting']

    system_identifier = get_system_identifier(system)
    timeout = settings['timeout']

    data: DataSet = experiment['data']
    query: Query = experiment['query']
    n_runs: int = settings['n_runs']

    # always set the home variable to the current home
    env_vars = {
        'HOME': os.environ['HOME']
    }

    script = get_experiment_script(system, data, query, system_settings, thread_index)

    runtimes = []
    cardinalities = []

    for i in range(n_runs):

        status: Status = run_script(system, script, timeout, thread_index, env_vars)

        if status != 'success':
            logger.error(f"Error in running {system['name']}-{system['version']}")
            # log the run script
            logger.error(script)
            sleep(0.2)  # wait for process to finish to release db locks
            if status == 'crash' or status == 'timeout':  # if timeout or crash, break
                break

        metrics_retrieved = system['get_metrics'](thread_index)
        if metrics_retrieved is None:
            logger.error(f"Error in retrieving metrics for {system['name']}-{system['version']}: {metrics_retrieved}")
            break
        else:
            duration, result_cardinality = metrics_retrieved

        runtimes.append(duration)
        cardinalities.append(result_cardinality)

    experiment_result['runtimes'] = runtimes
    experiment_result['cardinalities'] = cardinalities

    # save the results as a json file
    path = get_experiment_output_path_json(experiment)
    with open(path, 'w') as f:
        json.dump(experiment_result, f, indent=4, cls=SafeEncoder)
