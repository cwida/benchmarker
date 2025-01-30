import logging
import os
import sys

import psutil

from src.models import System, SystemBuildConfig
from src.utils import get_system_path, get_tmp_path

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

import os
import subprocess
from typing import Literal, Union, List


def build_systems(systems: Union[System, List[System]]):
    if isinstance(systems, list):
        for system in systems:
            build_system_if_necessary(system)
    else:
        build_system_if_necessary(systems)


def clone_repo(repository_url: str, path: str):
    clone_command = f'git clone "{repository_url}" "{path}"'

    if not os.path.exists(path):
        logging.info('Directory does not exist yet, cloning repo')
        os.system(clone_command)
    else:
        logging.info('Directory already exists, fetching the latest changes')
        # get path before changing directory
        path_before = os.getcwd()
        # perform a pull
        os.chdir(path)
        os.system('git fetch')
        # change back to the original directory
        os.chdir(path_before)

def clone_repo_and_checkout_commit(github_url: str, version_dir: str):

    # if version dir is empty, remove it
    if os.path.exists(version_dir) and len(os.listdir(version_dir)) == 0:
        logging.warning(f'Empty directory found at {version_dir} -> removing it')
        os.rmdir(version_dir)

    logging.info(f'Getting source code from {github_url} to {version_dir}')
    if '/commit/' in github_url:

        logging.info('The url points to a commit, checking out the commit')
        splitted = github_url.split('/commit/')
        repo = splitted[0] + '.git'
        commit = splitted[1]

        clone_repo(repo, version_dir)

        logging.info(f'Checking out commit: {commit}')
        checkout_command = f'git checkout {commit}'
        os.system(checkout_command)

    #'github_commit_url': 'https://github.com/gropaul/duckdb/tree/join-optimization/hash-marker-and-collision-bit'
    elif '/tree/' in github_url:

        logging.info('The url points to a branch, checking out the branch')
        splitted = github_url.split('/tree/')
        repo = splitted[0] + '.git'
        branch = splitted[1]

        clone_repo(repo, version_dir)

        logging.info(f'Checking out branch: {branch}')
        checkout_command = f'git checkout "{branch}"'
        os.system(checkout_command)
    else:
        logging.info('The url points to a repository, cloning the repository')
        clone_repo(github_url, version_dir)


def get_system_identifier(system: System) -> str:
    return system['name'] + '-' + system['version']


def build_system_if_necessary(system: System):

    if ('build_config' not in system) or (system['build_config'] is None):
        logging.info(f'No build config found for system {system["name"]} {system["version"]} -> skipping build')
        return

    build_config: SystemBuildConfig = system['build_config']
    system_location = build_config['location']

    if system_location['location'] == 'github':
        github_commit_url = system_location['github_url']
        repo_dir = get_system_path(system)
        clone_repo_and_checkout_commit(github_commit_url, repo_dir)
        os.chdir(repo_dir)

    build_command = build_config['build_command']

    # check if logging level is info or above
    if logging.getLogger().getEffectiveLevel() <= logging.INFO:
        logging.info(f'Building system {system["name"]} {system["version"]} with command: {build_command}')
        os.system(build_command)
    else:
        silent_command = f'{build_command} > /dev/null 2>&1'
        os.system(silent_command)

def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()


Status = Literal['success', 'timeout', 'crash']


def run_command_with_timeout(command: str, timeout: float, env_vars: dict = None) -> Status:
    logging.info(f'Running command: {command} with timeout {timeout}')
    logging.info(f'For running the command we have the following environment variables: {env_vars}')

    verbose = logging.getLogger().getEffectiveLevel() <= logging.INFO
    if not verbose:
        command += ' > /dev/null 2>&1'
    if verbose:
        proc = subprocess.Popen([command], shell=True, env=env_vars)
    else:
        proc = subprocess.Popen([command], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_vars)
    try:
        # Run the command with a timeout and capture output
        return_code = proc.wait(timeout=timeout)
        if return_code == 0:
            return 'success'
        logging.error(f'Command {command} failed with return code {return_code}')

    except subprocess.TimeoutExpired:
        kill(proc.pid)
        logging.error(f'Command {command} timed out after {timeout} seconds')
        return 'timeout'

    except subprocess.CalledProcessError as e:
        logging.error(f'Command {command} failed with error: {e}')
        return 'crash'
    except Exception as e:
        logging.error(f'Command {command} failed with error: {e}')
        return 'crash'

    return 'crash'


def run_script(system: System, script: str, timeout: float, thread_index: int, env_vars: dict) -> Status:
    repo_dir = get_system_path(system)
    run_comfig = system['run_config']
    if run_comfig['run_file_relative_to_build']:
        run_command = repo_dir + '/' + run_comfig['run_file']
    else:
        run_command = run_comfig['run_file']

    tmp_script_path = get_tmp_path(f'script-thread-{thread_index}.sql')
    with open(tmp_script_path, 'w') as f:
        f.write(script)

    total_command = f'{run_command} {tmp_script_path}'
    return run_command_with_timeout(total_command, timeout, env_vars=env_vars)


def cond_print(verbose: bool, message: str):
    if verbose:
        print(message)
