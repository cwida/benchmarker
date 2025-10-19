import os
import sys

from src.logger import get_logger
from src.models import System
from src.utils import get_tmp_path

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, root_directory)

import json
import os
from typing import Optional, Tuple

logger = get_logger(__name__)


def get_profile_path_duckdb(thread: int) -> str:
    return get_tmp_path(f'duckdb-profile-thread-{thread}.json')


def get_duckdb_profile_script(thread: int) -> str:
    path = get_profile_path_duckdb(thread)
    string = f"PRAGMA enable_profiling = 'json';pragma profile_output='{path}';"
    return string


def get_duckdb_runtime_and_cardinality(thread: int) -> Optional[Tuple[float, int]]:
    # load the json
    json_path = get_profile_path_duckdb(thread)

    # if the query crashed, the file does not exist
    if not os.path.exists(json_path):
        return None

    with open(json_path, 'r') as f:
        profile = json.load(f)

    if 'rows_returned' in profile:
        # if the cumulative cardinality is present, use it
        cardinality = profile['rows_returned']
        runtime = profile['latency']
    # get the runtime, can be either in the timing or operator_timing field
    elif 'timing' in profile:
        runtime = profile['timing']
        cardinality = profile['children'][0]['children'][0]['cardinality']
    elif 'operator_timing' in profile:
        runtime = profile['operator_timing']
        try:
            cardinality = profile['children'][0]['cardinality']
        except KeyError:  # Sometimes operator cardinality is not found.
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


DUCK_DB_BUILD_100: System = {
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
    'setup_script': 'PRAGMA disable_progress_bar;',
    'set_threads_command': lambda n_threads: f"PRAGMA threads = {n_threads};",
    'get_start_profiler_command': get_duckdb_profile_script,
    'get_metrics': get_duckdb_runtime_and_cardinality,
}

DUCK_DB_FACT_INTERSECTION_METRICS: System = {
    **DUCK_DB_BUILD_100,
    'version': 'fact-intersection',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/commit/446b25a1af4a39cede073f7f3872b49145ec2cd0',
        },
    },
}

DUCK_DB_FACT_INTERSECTION_NO_METRICS: System = {
    **DUCK_DB_BUILD_100,
    'version': 'fact-intersection-no-metrics',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/commit/446b25a1af4a39cede073f7f3872b49145ec2cd0'
        },
    },
}

DUCK_DB_LP_JOIN_BASELINE: System = {
    **DUCK_DB_BUILD_100,
    'version': 'baseline',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/commit/fd2e59672e02d49278e9491ed1bd8fa5d1cdb0a7'
        },
    },
}

DUCK_DB_LP_JOIN: System = {
    **DUCK_DB_BUILD_100,
    'version': 'lp-join',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/commit/e30f8270594e6fde06ca87b2193ad31d91db047e'
        },
    },
}

DUCK_DB_LP_JOIN_NO_SALT: System = {
    **DUCK_DB_BUILD_100,
    'version': 'lp-join-no-salt',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/commit/442274812bda9504b697524e58f796d182612838'
        },
    },
}

DUCK_DB_JOIN_OPTIMIZATION_BASELINE: System = {
    **DUCK_DB_BUILD_100,
    'version': 'join-optimization-baseline',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/commit/4ba2e66277a7576f58318c1aac112faa67c47b11'
        },
    },
}

DUCK_DB_JOIN_OPTIMIZATION_HASH_MARKER_AND_COLLISION_BIT: System = {
    **DUCK_DB_BUILD_100,
    'version': 'join-optimization-hash-marker-and-collision-bit',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join-optimization/hash-marker-and-collision-bit'
        },
    },
}

DUCK_DB_JOIN_OPTIMIZATION_HASH_MARKER: System = {
    **DUCK_DB_BUILD_100,
    'version': 'join-optimization-hash-marker',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join-optimization/hash-marker'
        },
    },
}

DUCK_DB_NIGHTLY: System = {
    **DUCK_DB_BUILD_100,
    'version': 'nightly',
    'build_config': None,
    'run_config': {
        'run_file_relative_to_build': False,
        'run_file': '/Users/paul/.local/bin/duckman run nightly <',
    }
}

DUCK_DB_NIGHTLY_BUILD_LOCALLY: System = {
    **DUCK_DB_BUILD_100,
    'version': 'nightly-build-locally',
    'build_config': None,
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/duckdb/duckdb'
        },
    },
}


DUCK_DB_V100: System = {
    **DUCK_DB_BUILD_100,
    'version': 'v1.0.0',
    'build_config': None,
    'run_config': {
        'run_file_relative_to_build': False,
        'run_file': '/Users/paul/.local/bin/duckman run 1.0.0 <',
    }
}

DUCK_DB_V113: System = {
    **DUCK_DB_BUILD_100,
    'version': 'v1.1.3',
    'build_config': None,
    'run_config': {
        'run_file_relative_to_build': False,
        'run_file': '/Users/paul/.local/bin/duckman run 1.1.3 <',
    }
}


DUCK_DB_WITHOUT_ATOMICS: System = {
    **DUCK_DB_BUILD_100,
    'version': 'without-atomics',
    'build_config': None,
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join/atomics-test'
        },
    },
}


DUCK_DB_PARTITIONED_NO_ATOMICS: System = {
    **DUCK_DB_BUILD_100,
    'version': 'partitioned-ht-no-atomics',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join/partioning-non-atomic-v2'
        },
    },
}


DUCK_DB_PARTITIONED_WITH_ATOMICS: System = {
    **DUCK_DB_BUILD_100,
    'version': 'partitioned-ht-with-atomics',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join/partitioned-build/v3'
        },
    },
}

DUCK_DB_OPTIONAL_PROBE_SEL: System = {
    **DUCK_DB_BUILD_100,
    'version': 'optional-probe-sel',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join/optional-probe-sel'
        },
    },
}


DUCK_DB_STRING_EQUALITY_V1: System = {
    **DUCK_DB_BUILD_100,
    'version': 'better-equality',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/strings/better-equality'
        },
    },
}

DUCK_DB_STRING_EQUALITY_BASELINE: System = {
    **DUCK_DB_BUILD_100,
    'version': 'better-equality-baseline',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/strings/baseline'
        },
    },
}

DUCK_DB_VALIDITY_DISABLED: System = {
    **DUCK_DB_BUILD_100,
    'version': 'validity-disabled',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/betterRowLayout/noValidityChecks'
        },
    },
}

DUCK_DB_VALIDITY_V1: System = {
    **DUCK_DB_BUILD_100,
    'version': 'validity-v1',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/betterRowLayout/v1'
        },
    },
}

DUCK_DB_VALIDITY_BASELINE: System = {
    **DUCK_DB_BUILD_100,
    'version': 'validity-baseline',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/betterRowLayout/baseline'
        },
    },
}


DUCK_DB_BF_BASELINE: System = {
    **DUCK_DB_BUILD_100,
    'version': 'bf-baseline',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/main'
        },
    },
}

DUCK_DB_BF_V1: System = {
    **DUCK_DB_BUILD_100,
    'version': 'bf-v1',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join/bf'
        },
    },
}

DUCK_DB_EARLY_PROBING: System = {
    **DUCK_DB_BUILD_100,
    'version': 'early-probing',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/join/early-probing-clean'
        },
    },
}

DUCK_DB_BF_RPT: System = {
    **DUCK_DB_BUILD_100,
    'version': 'bf-rpt',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/YimingQiao/duckdb/tree/predicate-transfer-PR'
        },
    },
}

DUCK_DB_ROW_MATCHER_OPTIONAL_NULL: System = {
    **DUCK_DB_BUILD_100,
    'version': 'optional-null',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/betterRowLayout/rowMatcherOptionalNullCheck'
        },
    },
}

DUCK_DB_ROW_MATCHER_BASELINE: System = {
    **DUCK_DB_BUILD_100,
    'version': 'baseline',
    'build_config': {
        **DUCK_DB_BUILD_100['build_config'],
        'location': {
            'location': 'github',
            'github_url': 'https://github.com/gropaul/duckdb/tree/betterRowLayout/baseline'
        },
    },
}
