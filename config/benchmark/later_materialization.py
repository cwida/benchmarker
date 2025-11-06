import os
import unittest
from typing import List, Tuple

import duckdb

from src.logger import get_logger
from src.models import DataSet, Benchmark, Query, Script
from src.utils import get_data_path, pad, DATA_PATH

logger = get_logger(__name__)

TOP_N = [1, 5, 10, 20, 50, 100, 200, 500, 1000, 5000, 10_000, 50_000, 100_000]
AVAILABLE_COLUMNS = [
    'l_orderkey',
    'l_partkey',
    'l_suppkey',
    'l_linenumber',
    'l_quantity',
    'l_extendedprice',
    'l_discount',
    'l_tax',
    'l_returnflag',
    'l_linestatus',
    'l_shipdate',
    'l_commitdate',
    'l_receiptdate',
    'l_shipinstruct',
    'l_shipmode',
    'l_comment'
]

SUBSET_COLUMNS = AVAILABLE_COLUMNS[:5]

COLUMN_SETS = [AVAILABLE_COLUMNS[:5], AVAILABLE_COLUMNS[:10], AVAILABLE_COLUMNS]
# COLUMN_SETS = [AVAILABLE_COLUMNS]

ORDER_COLUMNS = ['l_orderkey', 'l_orderkey DESC', 'hash(l_orderkey)', 'l_extendedprice', 'l_shipmode']

def get_queries_for_top_n_benchmark() -> List[Query]:
    queries = []
    for order_column in ORDER_COLUMNS:
        for columns in COLUMN_SETS:
            for n in TOP_N:
                query: Query = {
                    'name': f'top_{n}_order_by_{order_column}_columns_{"_".join(columns)}',
                    'index': len(queries),
                    'run_script': {
                        "duckdb": f"SELECT {', '.join(columns)} FROM lineitem ORDER BY {order_column} LIMIT {n};"
                    },
                    'config': {
                        'n': n,
                        'order_column': order_column,
                        'selected_columns': columns,
                    },
                }
                queries.append(query)

    return queries


def get_micro_late_materialization() -> Benchmark:
    datasets: List[DataSet] = __get_data()
    queries = get_queries_for_top_n_benchmark()
    return {
        'name': 'late_materialization',
        'datasets': datasets,
        'queries': queries
    }


def __get_data() -> List[DataSet]:
    duckdb_file_path = os.path.join(DATA_PATH, 'tpch', 'tpch-sf-10.db')
    setup_script: Script = {
        'duckdb': f"ATTACH '{duckdb_file_path}' (READ_ONLY); USE 'tpch-sf-10'; PRAGMA disable_progress_bar;"
    }

    dataset: DataSet = {
        'name': f'join-micro-probe-selectivity',
        'setup_script': setup_script,
        'config': {}
    }

    return [dataset]


if __name__ == "__main__":
    # This is just for testing the data generation
    get_micro_late_materialization()
    logger.info("Data generation finished.")
