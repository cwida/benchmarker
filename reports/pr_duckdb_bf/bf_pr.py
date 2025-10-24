
import duckdb

def create_pr_plots():

    runs_dir = '/Users/paul/workspace/benchmarker/_output/runs/duckdb_with_bf_micro/2025-10-23-15-16-50'

    jsons = duckdb.sql(f"""
        WITH data AS (
            SELECT 
                list_min(runtimes) as runtime, 
                experiment.system.version as version, 
                experiment.query.config.selectivity as selectivity,
                experiment.query.config.probe_cardinality as probe_cardinality,
                
            FROM '{runs_dir}/*.json'
        ), p AS (
            PIVOT data 
            ON version
            USING AVG(runtime) AS runtime
        ) 
        SELECT 
            selectivity, probe_cardinality, "bf-baseline_runtime" / "bf-x86_runtime" as speedup 
        FROM p
        ORDER BY probe_cardinality, selectivity
        
        
    """).df()

    probe_cardinalities = sorted(jsons['probe_cardinality'].unique())
    # plot the speedup vs selectivity
    import matplotlib.pyplot as plt
    plt.figure()
    for pc in probe_cardinalities:
        subset = jsons[jsons['probe_cardinality'] == pc]
        plt.plot(subset['selectivity'], subset['speedup'], marker='o', label=f'Probe Cardinality: {pc}')

    plt.xscale('log')
    plt.xlabel('Selectivity')
    plt.ylabel('Speedup (DuckDB Main vs DuckDB with BF)')

    # add a horizontal line at y=1
    plt.axhline(y=1, color='r', linestyle='--', label='No Speedup')

    # set x ticks to be 0.0001, 0.001, 0.01, 0.1, 1
    plt.xticks([0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0], ['0.01', '0.02', '0.05', '0.1', '0.2', '0.5', '1.0'])
    # the speedup y axis should go from 0 to the max speedup + 1
    plt.ylim(0, jsons['speedup'].max() + 0.5)
    plt.title('Speedup vs Selectivity for Different Probe Cardinalities')
    plt.grid(True, which="both", ls="--")
    plt.legend()


    plt.savefig('pr_speedup_plot.png')







if __name__ == "__main__":
    create_pr_plots()