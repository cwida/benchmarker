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


def system_dict_to_string(system_dict: dict[str, str]) -> str:
    """
    Convert a system dictionary to a string representation.
    Example: {'name': 'SystemA', 'version': '1.0'} -> "SystemA (1.0)"
    """
    name = system_dict.get('name', 'Unknown')
    if name == 'duckdb':
        name = 'DuckDB'

    version = system_dict.get('version', 'Unknown')

    version = version.replace('-', ' ')
    #capitalize the first letter of each word
    version = ' '.join(word.capitalize() for word in version.split())
    return f"{name} ({version})"

def dict_to_string(d: dict[str, str]) -> str:
    return str(d).replace('{', '').replace('}', '').replace("'", "").replace(': ', '=').replace(':', '=').replace(',', ', ')

def eval_system_tuple(system_tuple: tuple[dict[str, str], dict[str, str]], con: duckdb.DuckDBPyConnection, from_query: str) -> str:
    system_name_0 = system_dict_to_string(system_tuple[0])
    system_name_1 = system_dict_to_string(system_tuple[1])
    total_text = f"### {system_name_0} vs {system_name_1}\n"
    # Per Query
    total_text += eval_system_tuple_group(system_tuple, con, from_query, group_string='system_setting',
                                          group_string_label='System Setting')
    total_text += eval_system_tuple_group(system_tuple, con, from_query, group_string='data_config',
                                          group_string_label='Data Configuration')

    return total_text

def eval_system_tuple_group(system_tuple: tuple[dict[str, str], dict[str, str]], con: duckdb.DuckDBPyConnection, from_query: str, group_string: str = 'data_config', group_string_label: str = 'Data Configuration') -> str:
    system_name_0 = system_dict_to_string(system_tuple[0])
    system_name_1 = system_dict_to_string(system_tuple[1])
    text = f"##### Based on {group_string_label}\n"

    # Per Scale Factor
    query = f"""
        SELECT 
            CAST({group_string} AS STRING) as {group_string}_str, 
            AVG(min_runtime) as avg_runtime
        {from_query}
        AND system = '{str(system_tuple[0]).replace("'", "''")}'
        GROUP BY {group_string}
        ORDER BY {group_string};
    """
    df_0 = con.execute(query).fetchdf()

    query = f"""
        SELECT
            CAST({group_string} AS STRING) as {group_string}_str, 
            AVG(min_runtime) as avg_runtime
        {from_query}
        AND system = '{str(system_tuple[1]).replace("'", "''")}'
        GROUP BY {group_string}
        ORDER BY {group_string};
    """
    df_1 = con.execute(query).fetchdf()
    # Merge the two dataframes on group_string
    group_string = group_string + '_str'
    merged_df = pd.merge(df_0, df_1, on=group_string, suffixes=('_0', '_1'))
    # Calculate the percentage difference
    merged_df['percentage_diff'] = (merged_df['avg_runtime_0'] - merged_df['avg_runtime_1']) / merged_df['avg_runtime_1'] * 100
    # Create a new dataframe with the desired columns
    result_df = merged_df[[group_string, 'avg_runtime_0', 'avg_runtime_1', 'percentage_diff']]
    # reformat the data_config column
    result_df[group_string] = result_df[group_string].map(dict_to_string)
    result_df['avg_runtime_0'] = result_df['avg_runtime_0'].map( lambda x: f"{x:.4f}s")
    result_df['avg_runtime_1'] = result_df['avg_runtime_1'].map( lambda x: f"{x:.4f}s")
    result_df['percentage_diff'] = result_df['percentage_diff'].map(lambda x: f"{x:.2f}%")
    # Rename the columns
    result_df.columns = [group_string_label, f'{system_name_0} Runtime', f'{system_name_1} Runtime', 'Percentage Difference']

    # Convert the dataframe to a markdown table
    text += result_df.to_markdown(index=False, floatfmt=".2f")
    text += "\n\n"

    return text


def system_to_system_evaluation(con: duckdb.DuckDBPyConnection,
                                from_query: str) -> str:
    query = f"SELECT DISTINCT system {from_query}"
    df = con.execute(query).fetchdf()

    systems = df['system'].tolist()
    # order the systems by name
    systems.sort(key=lambda x: (x['name'], x['version']))
    # create all combinations of systems

    total_text = "\n## System to System Evaluation\n"

    for i in range(len(systems)):
        for j in range(i + 1, len(systems)):
            total_text += eval_system_tuple((systems[i], systems[j]), con, from_query)

    return total_text

def query_index_to_name_table(con: duckdb.DuckDBPyConnection, from_query: str) -> str:
    query = f"""
        SELECT DISTINCT query_index, query
        {from_query}
        ORDER BY query_index;
    """
    df = con.execute(query).fetchdf()
    # rename the columns
    df.columns = ['Query Index', 'Query']
    # Convert the dataframe to a markdown table
    text = "\n## Query Index to Name Table\n"
    text += df.to_markdown(index=False)
    text += "\n\n"
    return text


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
    system_plot_grouped_data_config = plot_aggregation('system', con, from_query, plots_path, per_query=True,
                                                       subplot_group='data_config')
    system_plot_grouped_system_setting = plot_aggregation('system', con, from_query, plots_path, per_query=True,
                                                          subplot_group='system_setting')
    system_plot = plot_aggregation('system', con, from_query, plots_path)

    system_setting_plot_grouped = plot_aggregation('system_setting', con, from_query, plots_path, per_query=True)
    system_setting_plot_grouped_by_system = plot_aggregation('system_setting', con, from_query, plots_path, per_query=True, subplot_group='system')
    system_setting_plot = plot_aggregation('system_name', con, from_query, plots_path)
    data_plot_grouped = plot_aggregation('data_config', con, from_query, plots_path, per_query=True)
    data_plot = plot_aggregation('query', con, from_query, plots_path)

    s2s_text = system_to_system_evaluation(con, from_query)



    # create little markdown file with embedded plots, we can create md images as ![name](path)
    md = f"""
# {run_name} - {run_date}
"""
    md += s2s_text

    md += query_index_to_name_table(con, from_query)

    plots_md = f"""
## Performance per System and Data Configuration
![System](plots/{os.path.basename(system_plot_grouped_data_config)})
## Performance per System and System Configuration
![System](plots/{os.path.basename(system_setting_plot_grouped_by_system)})
## Performance per System Setting
![System Setting](plots/{os.path.basename(system_setting_plot_grouped)})
## Performance per Data Configuration
![Data Configuration](plots/{os.path.basename(data_plot_grouped)})
"""
    # add the plots to the markdown
    md += plots_md

    with open(os.path.join(path, 'Summary.md'), 'w') as f:
        f.write(md)


import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import duckdb


def plot_aggregation(group_column: str,
                     con: duckdb.DuckDBPyConnection,
                     from_query: str,
                     path: str,
                     per_query=False,
                     subplot_group: str | None = None):
    """
    Plots average runtimes aggregated by either:
      - just the group_column if per_query=False
      - query_index * group_column if per_query=True (grouped bar plot)
      - one subplot per unique value of subplot_group if provided
    """

    # clear any existing figures before we start
    plt.close('all')

    # Build the query, optionally including query_index and subplot_group in SELECT, GROUP BY, ORDER BY
    aggregated_query = f"""
        SELECT 
            replace(CAST({group_column} AS STRING)[2:-2], '''', '') as group_column_string
            {', (query_index + 1) as query_index' if per_query else ''},
            AVG(min_runtime) as avg_runtime
            {', replace(CAST(' + subplot_group + " AS STRING)[2:-2], '''', '') as " + subplot_group + '_str' if subplot_group else ''} 
        {from_query}
        GROUP BY {group_column} 
                 {', query_index' if per_query else ''} 
                 {(',' + subplot_group) if subplot_group else ''}
        ORDER BY {group_column} 
                 {', query_index' if per_query else ''}
                 {(',' + subplot_group) if subplot_group else ''};
    """

    # Execute and fetch data into a pandas DataFrame
    df_aggregated = con.execute(aggregated_query).fetchdf()

    # Ensure plot directory exists
    if not os.path.exists(path):
        os.makedirs(path)

    if subplot_group is None:
        # Just create a single “main” plot
        return create_plot(
            df_aggregated,
            group_column,
            path,
            per_query
        )
    else:
        # We have subgroups to plot in separate subplots
        unique_subgroups = df_aggregated[subplot_group + '_str'].unique()

        width = get_plot_width(df_aggregated, per_query)

        # Create one figure with N subplots (one per unique value)
        fig, axes = plt.subplots(nrows=len(unique_subgroups), ncols=1, figsize=(width, 5 * len(unique_subgroups)))

        # If there is only one subgroup, `axes` is a single Axes object instead of a list
        if len(unique_subgroups) == 1:
            axes = [axes]

        # For each unique subgroup, filter the DataFrame and plot on a dedicated subplot
        for i, subgroup_value in enumerate(unique_subgroups):
            ax = axes[i]
            subset_df = df_aggregated[df_aggregated[subplot_group + '_str'] == subgroup_value]
            # Pass in `ax` and a subplot title
            create_plot(
                subset_df,
                group_column,
                path,
                per_query,
                ax=ax,
                subplot_title=f"{subplot_group} = {subgroup_value}",
                # Name extension so CSV/PNG from each subplot is traceable if you want it
                # but here we omit it so we do not re-save multiple times from create_plot
                name_extension=""
            )

        plt.tight_layout()
        final_plot_path = os.path.join(path, f"{group_column}_subplots_by_{subplot_group}.png")
        plt.savefig(final_plot_path, bbox_inches='tight', pad_inches=0.1, dpi=300)
        plt.close(fig)

        return final_plot_path


def get_plot_width(df: pd.DataFrame, per_query: bool) -> float:
    if per_query:
        # If per_query, the width is the number of queries times a fixed width per query
        return len(df['query_index'].unique()) * 0.8
    else:
        # If not per_query, the width is the number of unique group_column values times a fixed width per value
        return 8.0


def create_plot(df_aggregated: pd.DataFrame,
                group_column: str,
                path: str,
                per_query: bool,
                name_extension: str = '',
                ax: plt.Axes = None,
                subplot_title: str = None
                ) -> str:
    """
    Draws either a single bar plot or a grouped bar plot (depending on per_query).
    If an `ax` (Axes) is provided, the plot is drawn on that subplot and no figure is saved.
    Otherwise, this function creates its own figure and saves it as a PNG (and writes out a CSV).
    """

    if ax is None:
        # We create a brand-new figure for a “standalone” plot
        plt.close('all')  # defensive
        width = get_plot_width(df_aggregated, per_query)
        fig, ax = plt.subplots(figsize=(width, 5))
        is_standalone = True
    else:
        # We are drawing into an existing subplot
        is_standalone = False

    # Convert to numeric if needed
    df_aggregated['avg_runtime'] = pd.to_numeric(df_aggregated['avg_runtime'], errors='coerce')

    if per_query:
        # Grouped bar plot with query_index on the x-axis
        df_pivot = df_aggregated.pivot(
            index='query_index',
            columns='group_column_string',
            values='avg_runtime'
        ).reindex(columns=df_aggregated['group_column_string'].unique())

        x = np.arange(len(df_pivot.index))  # the label locations (one per query_index)
        width = 0.8 / len(df_pivot.columns)  # width of each sub-bar
        multiplier = 0

        for col_name in df_pivot.columns:
            offset = multiplier * width
            rects = ax.bar(x + offset, df_pivot[col_name], width, label=col_name)
            ax.bar_label(rects, padding=3, fmt='%.2f')
            multiplier += 1

        # X-ticks
        ax.set_xticks(x + width * (len(df_pivot.columns) - 1) / 2, df_pivot.index)

        ax.set_xlabel("Query Index")
        ax.set_ylabel("Average Runtime")
        ax.set_title(f'Average Runtime by Query Index grouped by {group_column}')
        ax.legend(loc='upper left')

        # If we’re a standalone figure, save plot & data
        if is_standalone:
            plot_path = os.path.join(path, f'{group_column}_per_query{name_extension}.png')
            plt.savefig(plot_path, bbox_inches='tight', pad_inches=0.1, dpi=300)
            # Save the data
            df_pivot.to_csv(os.path.join(path, f'{group_column}_per_query{name_extension}.csv'))
            plt.close(fig)
            return plot_path
        else:
            # If we’re in a subplot, optionally set a subplot title
            if subplot_title:
                ax.set_title(subplot_title)
            return ""  # not saving from here

    else:
        # A simple bar plot for the aggregated data
        x_labels = df_aggregated['group_column_string']
        y_values = df_aggregated['avg_runtime']
        ax.bar(x_labels, y_values)

        # Add labels on top of each bar
        for i, v in enumerate(y_values):
            label = 'nan' if pd.isnull(v) else f'{v:.2f}'
            ax.text(i, 0 if pd.isnull(v) else v, label,
                    ha='center', va='bottom')

        ax.set_xlabel(group_column)
        ax.set_ylabel('Average Runtime')
        ax.set_title(f'Average Runtime by {group_column}')

        if is_standalone:
            plot_path = os.path.join(path, f'{group_column}{name_extension}.png')
            plt.tight_layout()
            plt.savefig(plot_path, bbox_inches='tight', pad_inches=0.1, dpi=300)
            df_aggregated.to_csv(os.path.join(path, f'{group_column}{name_extension}.csv'), index=False)
            plt.close(fig)
            return plot_path
        else:
            if subplot_title:
                ax.set_title(subplot_title)
            return ""


if __name__ == "__main__":
    run_evaluation()
