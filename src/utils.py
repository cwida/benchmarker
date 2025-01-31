import json
import os
import sys

from src.models import System, Experiment

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

from pathlib import Path


def find_project_root(current_path, marker='.git'):
    current_path = Path(current_path).resolve()
    for parent in current_path.parents:
        if (parent / marker).exists():
            return parent
    return None  # Or raise an error if the root is not found

SEED = 42


# use the DATA_PATH environment variable to get the path to the data directory
ROOT_DIR_PATH = os.getenv('EXPERIMENTS_DIR_PATH', find_project_root(__file__))
_OUTPUT_DIR_PATH = os.path.join(ROOT_DIR_PATH, '_output')
DATA_PATH = os.path.join(_OUTPUT_DIR_PATH, 'data')
SYSTEMS_PATH = os.path.join(_OUTPUT_DIR_PATH, 'systems')
EXPERIMENT_RUNS_PATH = os.path.join(_OUTPUT_DIR_PATH, 'runs')
RESULTS_PATH = os.path.join(_OUTPUT_DIR_PATH, 'results')
TMP_PATH = os.path.join(_OUTPUT_DIR_PATH, 'tmp')


dirs = [
    DATA_PATH,
    _OUTPUT_DIR_PATH,
    SYSTEMS_PATH,
    EXPERIMENT_RUNS_PATH,
    TMP_PATH,
]

DESCRIPTION_LENGTH = 25


def pad(description: str) -> str:
    return description.rjust(DESCRIPTION_LENGTH, ' ')


for dir_name in dirs:
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def get_tmp_path(path: str) -> str:
    return os.path.join(TMP_PATH, path)

def get_snb_path(sf: int) -> str:
    return os.path.join(DATA_PATH, f'SNB{sf}-projected|')

def get_snb_parquet_path(sf: int) -> str:
    return os.path.join(DATA_PATH, f'SNB{sf}-parquet')


def get_data_path(path: str) -> str:
    path = os.path.join(DATA_PATH, path)
    parent = os.path.dirname(path)
    if not os.path.exists(parent):
        os.makedirs(parent)
    return path


def get_system_output_path(system: System) -> str:
    path = system['name'] + '-' + system['version']
    return os.path.join(EXPERIMENT_RUNS_PATH, path)

def get_system_path(system: System) -> str:
    path = system['name'] + '-' + system['version']
    return os.path.join(SYSTEMS_PATH, path)


def get_exeriment_runs_path(experiment: Experiment) -> str:
    path = os.path.join(EXPERIMENT_RUNS_PATH, experiment['run_name'])
    if not os.path.exists(path):
        os.makedirs(path)
    return path

def get_experiment_file_name(experiment: dict) -> str:
    # hash the name to avoid long names, add the timestamp to avoid overwriting

    name_hashed = f'{experiment["name"]}'
    return name_hashed

def get_experiment_output_path_json(experiment: Experiment) -> str:
    result_dir = get_exeriment_runs_path(experiment)

    json_dir = os.path.join(result_dir, experiment['run_date'])

    if not os.path.exists(json_dir):
        os.makedirs(json_dir)


    path = get_experiment_file_name(experiment) + '.json'

    return os.path.join(json_dir, path)


def get_experiment_output_path_csv(experiment: Experiment) -> str:
    result_dir = get_exeriment_runs_path(experiment)

    csv_dir = os.path.join(result_dir, 'csv')

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    path = get_experiment_file_name(experiment) + '.csv'

    return os.path.join(csv_dir, path)


def get_plot_path(overall_name: str, plot_type: str, plot_name: str) -> str:
    path = os.path.join(EXPERIMENT_RUNS_PATH, 'result', overall_name)
    if not os.path.exists(path):
        os.makedirs(path)

    return os.path.join(path, f'{plot_type}-{plot_name}.png')



class SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return None  # or you can choose to return a placeholder like str(obj) or skip it by returning None