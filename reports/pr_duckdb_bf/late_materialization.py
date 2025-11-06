def main():
    import pandas as pd
    import matplotlib.pyplot as plt

    data = '/Users/paul/workspace/benchmarker/_output/results/duckdb_with_bf_late_materialization/2025-11-05-10-20-07/run.csv'
    df = pd.read_csv(data)

    # extract systems
    systems = df['system_version'].unique().tolist()

    # parse query_config once to avoid repeating eval
    # query_config: {'n': 1, 'order_column': 'l_orderkey', 'selected_columns': [...]}
    def parse_qc(x):
        return eval(x)  # assuming trusted source, same as your code
    df['_qc'] = df['query_config'].apply(parse_qc)

    # add helper columns
    df['n_columns'] = df['_qc'].apply(lambda qc: len(qc['selected_columns']))
    df['order_column'] = df['_qc'].apply(lambda qc: qc['order_column'])
    df['n_value'] = df['_qc'].apply(lambda qc: qc['n'])

    # all order columns present in the file
    order_columns = df['order_column'].unique().tolist()

    # iterate per number of selected columns
    for n_columns in sorted(df['n_columns'].unique().tolist()):
        df_n = df[df['n_columns'] == n_columns]

        columns_string = ', '.join(
            df_n['_qc'].iloc[0]['selected_columns']
        )

        num_orders = len(order_columns)
        # make one figure with subplots, 1 per order_column
        fig, axs = plt.subplots(
            1, num_orders,
            figsize=(5 * num_orders, 4),
            squeeze=False
        )
        axs = axs[0]  # single row

        for idx, order_col in enumerate(order_columns):
            ax = axs[idx]
            df_order = df_n[df_n['order_column'] == order_col]

            # plot each system
            for system in systems:
                df_sys = df_order[df_order['system_version'] == system].copy()
                if df_sys.empty:
                    continue
                # sort by n
                df_sys = df_sys.sort_values(by='n_value')
                n_values = df_sys['n_value'].tolist()
                runtimes = df_sys['min_runtime'].tolist()

                print(f'System: {system}, n_columns: {n_columns}, order: {order_col}')
                print(n_values)
                print(runtimes)

                ax.plot(n_values, runtimes, marker='o', label=system)

            ax.set_xscale('log')
            ax.set_yscale('log')
            ax.set_xlabel('n (LIMIT)')
            ax.set_title(f'order by {order_col}')
            ax.grid(True, which="both", ls="--")
            # y-ticks like before
            ax.set_yticks(
                [0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1]
            )
            ax.set_yticklabels(
                ['1ms','5ms', '10ms', '20ms', '50ms', '100ms', '200ms', '500ms', '1s']
            )
            ax.set_ylim(0.001, 2)

            if idx == 0:
                # put legend on the last subplot (or you can move it globally)
                ax.legend()

        fig.suptitle(f'{n_columns} Columns Selected: {columns_string}', fontsize=12)
        fig.tight_layout()

        # save per n_columns
        fig.savefig(f'late_materialization_{n_columns}_columns_all_orders.png', bbox_inches='tight')


if __name__ == "__main__":
    main()
