import os

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from src.utils import EXPERIMENT_RUNS_PATH, RESULTS_PATH
import numpy as np


def run_evaluation():
    runs_path = EXPERIMENT_RUNS_PATH
    con = duckdb.connect(':memory:')

    view_query = f"""
        CREATE OR REPLACE VIEW intermediate AS (
            SELECT 
                experiment.run_name as run_name,
                experiment.run_date as run_date,
                experiment.data.config as data_config,
                experiment.query.name as query,
                experiment.query.index as query_index,
                experiment.system.name as system_name,
                experiment.system.version as system_version,
                {{'name': system_name, 'version': system_version}} as system,
                experiment.system_setting as system_setting,
                list_min(runtimes) as min_runtime
            FROM '{runs_path}/*/*/*.json'
        );"""
    con.execute(view_query)

    unique_names = con.execute('SELECT DISTINCT run_name FROM intermediate').fetchdf()
    for run_name in unique_names['run_name']:
        evaluate_run(run_name, con)


def evaluate_run(run_name: str, con: duckdb.DuckDBPyConnection):
    query = f"SELECT * FROM intermediate WHERE run_name = '{run_name}'"

    # save total run data
    path = os.path.join(RESULTS_PATH, run_name)
    if not os.path.exists(path):
        os.makedirs(path)
    df = con.execute(query).fetchdf()
    df.to_csv(os.path.join(path, 'benchmark.csv'), index=False)

    unique_dates = con.execute(f"SELECT DISTINCT run_date FROM intermediate WHERE run_name = '{run_name}'").fetchdf()
    run_dates = unique_dates['run_date']
    for run_date in run_dates:
        evaluate_run_date(run_name, run_date, con)


def evaluate_run_date(run_name: str, run_date: str, con: duckdb.DuckDBPyConnection):
    # Step 1: Fetch initial data
    from_query = f"FROM intermediate WHERE run_name = '{run_name}' AND run_date = '{run_date}'"
    df = con.execute(f"SELECT * {from_query}").fetchdf()

    # Step 2: Save initial data to CSV
    path = os.path.join(RESULTS_PATH, run_name, run_date)
    plots_path = os.path.join(path, 'plots')
    if not os.path.exists(plots_path):
        os.makedirs(plots_path)
    df.to_csv(os.path.join(path, 'run.csv'), index=False)

    system_plot_grouped = plot_aggregation('system', con, from_query, plots_path, per_query=True)
    system_plot = plot_aggregation('system', con, from_query, plots_path)


    system_setting_plot_grouped = plot_aggregation('system_setting', con, from_query, plots_path, per_query=True)
    system_setting_plot = plot_aggregation('system_name', con, from_query, plots_path)
    data_plot_grouped = plot_aggregation('data_config', con, from_query, plots_path, per_query=True)
    data_plot = plot_aggregation('query', con, from_query, plots_path)





    # create little markdown file with embedded plots, we can create md images as ![name](path)
    md = f"""
# {run_name} - {run_date}
## Performance per System
![System](plots/{os.path.basename(system_plot_grouped)})
## Performance per System Setting
![System Setting](plots/{os.path.basename(system_setting_plot_grouped)})
## Performance per Data Configuration
![Data Configuration](plots/{os.path.basename(data_plot_grouped)})

    """
    with open(os.path.join(path, 'Summary.md'), 'w') as f:
        f.write(md)


def plot_aggregation(group_column: str, con: duckdb.DuckDBPyConnection, from_query: str, path: str, per_query=False):
    """
    Plots average runtimes aggregated by either:
      - just the group_column if per_query=False
      - query_index * group_column if per_query=True (grouped bar plot)
    """
    # Step 4: Aggregated data query
    # For per_query=True, we also bring in (query_index + 1) as query_index

    # clear all figures before plotting
    plt.close('all')

    aggregated_query = f"""
        SELECT 
            replace(CAST({group_column} AS STRING)[2:-2], '''', '') as group_column_string
            {', (query_index + 1) as query_index' if per_query else ''},
            AVG(min_runtime) as avg_runtime
        {from_query}
        GROUP BY {group_column} {', query_index' if per_query else ''}
        ORDER BY {group_column} {', query_index' if per_query else ''};
    """

    df_aggregated = con.execute(aggregated_query).fetchdf()

    number_of_groups = len(df_aggregated)
    width_per_group = 0.6
    plot_width = max(width_per_group * number_of_groups, 5)

    # Create plot directory if it doesn't exist
    if not os.path.exists(path):
        os.makedirs(path)

    if per_query:
        # We want a grouped bar plot with query_index on the x-axis
        # and each group_column_string as a separate bar in each group.

        # Pivot the DataFrame so:
        #   - rows are query_index
        #   - columns are group_column_string
        #   - values are avg_runtime
        # keep the order of the groups as they are in the original DataFrame
        df_pivot = df_aggregated.pivot(
            index='query_index',
            columns='group_column_string',
            values='avg_runtime'
        ).reindex(columns=df_aggregated['group_column_string'].unique())

        x = np.arange(len(df_pivot.index))  # the label locations (one per query_index)
        # Dynamically size the bar width based on the number of groups:
        width = 0.8 / len(df_pivot.columns)

        fig, ax = plt.subplots(figsize=(plot_width, 5))
        multiplier = 0

        for col_name in df_pivot.columns:
            offset = multiplier * width
            # df_pivot[col_name] may contain NaN for missing data; matplotlib will skip those
            rects = ax.bar(x + offset, df_pivot[col_name], width, label=col_name)
            # Add labels to each bar
            ax.bar_label(rects, padding=3, fmt='%.2f')
            multiplier += 1

        # Center x-ticks across all sub-bars in a group
        ax.set_xticks(x + width * (len(df_pivot.columns) - 1) / 2, df_pivot.index)

        ax.set_xlabel("Query Index")
        ax.set_ylabel("Average Runtime")
        ax.set_title(f'Average Runtime by Query Index grouped by {group_column}')
        ax.legend(loc='upper left')

        # Save the plot
        plot_path = os.path.join(path, f'{group_column}_grouped.png')
        plt.savefig(plot_path, bbox_inches='tight', pad_inches=0.1, dpi=300)

        # Save the aggregated pivot table to CSV (wide format)
        df_pivot.to_csv(os.path.join(path, f'{group_column}_grouped.csv'))
        plt.close(fig)  # Close the figure to avoid memory issues with multiple plots

        return plot_path

    else:
        # Standard single bar plot (no query grouping)
        plt.figure(figsize=(plot_width, 5))
        plt.bar(df_aggregated['group_column_string'], df_aggregated['avg_runtime'])
        for i, v in enumerate(df_aggregated['avg_runtime']):
            plt.text(i, v if not pd.isnull(v) else 0,
                     str(round(v, 2)) if not pd.isnull(v) else 'nan',
                     ha='center', va='bottom')

        plt.xlabel(group_column)
        plt.ylabel('Average Runtime')
        plt.title(f'Average Runtime by {group_column}')

        # Save the plot
        plot_path = os.path.join(path, f'{group_column}.png')
        plt.tight_layout()
        plt.savefig(plot_path, bbox_inches='tight', pad_inches=0.1)

        # Also save the raw aggregated data
        df_aggregated.to_csv(os.path.join(path, f'{group_column}.csv'), index=False)
        plt.close()

        return plot_path


if __name__ == "__main__":
    run_evaluation()
