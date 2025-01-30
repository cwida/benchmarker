import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from src.utils_eval import load_results
from utils import EXPERIMENT_BASE_NAME_LP_JOIN, get_results_path

# set font size for all plots
plt.rcParams.update({'font.size': 14})

name = 'name'
query_name = 'query_name'
column_duckdb_runtime = 'duckdb-main-avg-runtime'
column_duckdb_adaptive_runtime = 'duckdb-fact-intersection-avg-runtime'

n_chains = 'duckdb-fact-intersection-n_chains'
n_rows = 'duckdb-fact-intersection-n_rows'
avg_chain_length = 'avg-chain-length'

speed_up = 'speedup'
faster = 'faster'

BENCHMARKS = ['tpch', 'tpcds']
BENCHMARKS = ['tpch']

COLORS = ['#0D99FF', '#14AE5C', '#FFA629']

# Enable LaTeX-style fonts in the plot
plt.rc('text', usetex=True)
plt.rc('font', family='serif')
# set font size for all plots
plt.rcParams.update({'font.size': 16})

def add_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Split name column into two columns: dataset_base and scale_factor
    df['dataset_base'] = df['name'].apply(lambda x: x.split('$$')[0])
    df['query_name'] = df['name'].apply(lambda x: x.split('$$')[1])

    df['scale_factor'] = df['dataset_base'].apply(lambda x: x.split('-')[1])

    # Extract benchmark and query_number from query_name
    df['benchmark'] = df['query_name'].apply(lambda x: 'tpch' if 'tpch' in x else 'tpcds')
    df['query_number'] = df['query_name'].str.replace('tpch', '').str.replace('tpcds', '')

    # make sure the query_number is an integer
    df['query_number'] = df['query_number'].astype(int)

    # make sure the scale_factor is an integer
    df['scale_factor'] = df['scale_factor'].astype(int)

    # add relative runtimes
    columns = df.columns
    median_runtime_columns = [col for col in columns if 'median-runtime' in col]
    baseline_column = [col for col in median_runtime_columns if 'baseline' in col][0]

    for column in median_runtime_columns:
        name_rel = column + '-rel'
        df[name_rel] = df[column] / df[baseline_column]

    return df.drop(columns=['dataset_base'])


def plot_total_runtime(df: pd.DataFrame, base_name: str):
    columns = df.columns
    dir_path = os.path.join(get_results_path({"overall_name": base_name}), 'plots')

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    scale_factors = df['scale_factor'].unique()
    scale_factors.sort()

    sf_columns = [f"SF {sf}" for sf in scale_factors]
    out_columns = ['System'] + sf_columns
    output_df = pd.DataFrame(columns=out_columns)

    # order by benchmark, scale_factor, query_number
    df = df.sort_values(by=['benchmark', 'scale_factor', 'query_number'])
    for benchmark in BENCHMARKS:
        df_data = df[(df['benchmark'] == benchmark)]

        median_runtime_columns = [col for col in columns if 'median-runtime' in col and 'rel' not in col]
        median_runtime_columns.sort(key=lambda x: 'baseline' not in x)

        for (j, column) in enumerate(median_runtime_columns):
            column_runtimes = []
            for sf in scale_factors:
                df_sf = df_data[df_data['scale_factor'] == sf]
                sum = df_sf[column].sum()
                column_runtimes.append(sum)

            plt.plot(scale_factors, column_runtimes, label=get_label_for_column(column), marker='o', color=COLORS[j])
            out_row = [get_label_for_column(column)] + column_runtimes
            output_df.loc[len(output_df)] = out_row

        plt.xlabel('Scale Factor')
        plt.ylabel('Total Runtime (s)')

        # both log scale
        plt.yscale('log')
        plt.xscale('log')

        plt.legend()

        plt.tight_layout()

        plt.savefig(os.path.join(dir_path, f'{benchmark}_runtime_absolute.pdf'))
        plt.clf()

    # make all the columns in the output_df to be string with 2 decimal places and percentage
    # Convert to float if not already numeric
    for sf in sf_columns:
        output_df[sf] = pd.to_numeric(output_df[sf], errors='coerce')  # Converts non-convertible values to NaN

    # make everything to 4 decimal places
    output_df[sf_columns] = output_df[sf_columns].map(lambda x: f'{x:.2f}')

    # save the output_df as latex table
    output_df.to_latex(os.path.join(dir_path, f'{benchmark}_runtime_absolute.tex'), index=False)


def create_grouped_bar_chart(categories, groups, group_labels=None, bar_width=0.5, name='plot'):
    plt.clf()

    num_groups = len(groups)
    num_categories = len(categories)

    x_size = 7
    y_size = 0.8 * num_categories

    plt.figure(figsize=(x_size, y_size))
    plt.grid(axis='x')

    # Create labels for groups if not provided
    if group_labels is None:
        group_labels = [f'Group {i + 1}' for i in range(num_groups)]

    # Color interpolation (lerp)
    colors = plt.cm.viridis(np.linspace(0, 1, num_groups))

    # The position of the bars on the y-axis
    r = [np.arange(num_categories) * (bar_width * (num_groups + 1))]
    for i in range(1, num_groups):
        r.append([x + bar_width for x in r[i - 1]])

    # Create the bars
    for i in range(num_groups):
        plt.barh(r[i], groups[i], height=bar_width, color=colors[i], label=group_labels[i])

    # Add labels
    plt.xlabel('Speedup')
    plt.ylabel('Query')
    plt.yticks([r[0][i] + bar_width * (num_groups - 1) / 2 for i in range(num_categories)], categories)

    # Add a baseline line at x=1
    plt.axvline(x=1, color='red', linestyle='--', label='Baseline')

    # max x = 1.25, min x = 0.75
    plt.xlim(0.70, 1.3)
    # ticks for the x axis
    plt.xticks(np.arange(0.75, 1.26, 0.05))
    # Add a legend
    plt.legend()


    plt.tight_layout()

    # Ensure the plots directory exists
    os.makedirs('plots', exist_ok=True)

    # Save the plot
    plt.savefig(f'plots/{name}.pdf')


    plt.clf()

def get_label_for_column(column: str) -> str:
    # remove median-runtime
    label = column.replace('-median-runtime', '')
    # remove duckdb
    label = label.replace('duckdb-', '')

    # replace - with space
    label = label.replace('-', ' ')
    print(column)
    if 'baseline' in label:
        return 'Baseline'
    elif 'salt' in label:
        label = 'LC'
    else:
        label = 'LC + salt'

    return label


def plot_total_runtime_rel(df: pd.DataFrame, base_name: str):
    columns = df.columns
    dir_path = os.path.join(get_results_path({"overall_name": base_name}), 'plots')

    # order by benchmark, scale_factor, query_number
    df = df.sort_values(by=['benchmark', 'scale_factor', 'query_number'])

    scale_factors = df['scale_factor'].unique()
    scale_factors.sort()

    sf_columns = [f"SF {sf}" for sf in scale_factors]
    out_columns = ['System'] + sf_columns
    output_df = pd.DataFrame(columns=out_columns)

    # sf_columns are float columns
    for sf in sf_columns:
        output_df[sf] = output_df[sf].astype(float)

    for benchmark in BENCHMARKS:
        df_data = df[(df['benchmark'] == benchmark)]

        median_runtime_columns = [col for col in columns if 'median-runtime' in col and 'rel' not in col]

        # sort the columns so that the baseline is the first
        median_runtime_columns.sort(key=lambda x: 'baseline' not in x)

        baseline_column = [col for col in median_runtime_columns if 'baseline' in col][0]
        baseline_runtime = []

        for sf in scale_factors:
            df_sf = df_data[df_data['scale_factor'] == sf]
            sum = df_sf[baseline_column].sum()
            baseline_runtime.append(sum)

        queries = df_data['query_number'].unique()
        # sort reverse so that the queries are in the correct order
        queries.sort()
        queries = queries[::-1]

        categories = [f'Q{query}' for query in queries]

        ration = 5.0 / 4
        size = 4
        # Create the plot
        plt.figure(figsize=(4.5,3.3))

        for (j, column) in enumerate(median_runtime_columns):

            column_runtimes = []

            groups = []
            for (i, sf) in enumerate(scale_factors):
                df_sf = df_data[df_data['scale_factor'] == sf]
                sum = df_sf[column].sum()

                rel = baseline_runtime[i] / sum
                column_runtimes.append(rel)

                sf_group = []
                for query in queries:
                    df_query = df_sf[df_sf['query_number'] == query]
                    runtime_query = df_query[column].sum()
                    baseline_runtime_query = df_query[baseline_column].sum()
                    print(f'{benchmark} {column} {sf} {query} {runtime_query} {baseline_runtime}')

                    speed_up_query = baseline_runtime_query / runtime_query
                    sf_group.append(speed_up_query)

                groups.append(sf_group)

            # if baseline skip
            if 'baseline' in column:
                continue

            name = f'{benchmark}_{column}'
            scale_factor_labels = [f'SF {sf}' for sf in scale_factors]
            # create_grouped_bar_chart(categories, groups, group_labels=scale_factor_labels, name=name, bar_width=1.0)

            # add red horizontal line at 1
            plt.axhline(y=1, color='#F24822', linestyle='--')
            plt.plot(scale_factors, column_runtimes, label=get_label_for_column(column), marker='o', color=COLORS[j])
            out_row = [get_label_for_column(column)] + column_runtimes
            output_df.loc[len(output_df)] = out_row

        plt.xlabel('Scale Factor')
        plt.ylabel('Speedup')

        # both log scale
        plt.xscale('log')

        # add legend
        plt.legend()

        # only have 3 ticks on the y axis
        # plt.yticks([0.9, 0.95, 1.0])
        plt.tight_layout()

        # Save the plot
        plt.savefig(os.path.join(dir_path, f'{benchmark}_runtime_relative.pdf'), bbox_inches='tight', pad_inches=0.05)
        plt.savefig(os.path.join(dir_path, f'{benchmark}_runtime_relative.png'), bbox_inches='tight', pad_inches=0.05)
        plt.clf()
    print(output_df)

    # make all the columns in the output_df to be string with 2 decimal places and percentage
    # Convert to float if not already numeric
    for sf in sf_columns:
        output_df[sf] = pd.to_numeric(output_df[sf], errors='coerce')  # Converts non-convertible values to NaN

    # Apply formatting
    output_df[sf_columns] = output_df[sf_columns].map(lambda x: f'{x * 100:.2f}\\%')
    # save the output_df as latex table
    output_df.to_latex(os.path.join(dir_path, f'{benchmark}_runtime_relative.tex'), index=False)


def main():
    # get the results as a dataframe
    df = load_results(EXPERIMENT_BASE_NAME_LP_JOIN)
    df = add_columns(df)

    plot_total_runtime(df, EXPERIMENT_BASE_NAME_LP_JOIN)
    plot_total_runtime_rel(df, EXPERIMENT_BASE_NAME_LP_JOIN)

    df.to_csv('results.csv', index=False)


if __name__ == '__main__':
    main()
