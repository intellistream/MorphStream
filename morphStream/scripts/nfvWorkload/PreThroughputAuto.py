import os
import pandas as pd
import matplotlib.pyplot as plt

def read_and_plot(root_directory):
    # Define the pattern names and CC strategy names
    patterns = ["loneOperative", "sharedReaders", "sharedWriters", "mutualInteractive"]
    cc_strategies = ["Partitioning", "Replication", "Offloading", "Preemptive"]

    # Prepare the structure to hold data
    data = {pattern: {} for pattern in patterns}

    # Iterate over the patterns and ccStrategies
    for pattern in patterns:
        for strategy in cc_strategies:
            file_path = f"{root_directory}/{pattern}/{strategy}.csv"

            # Read the CSV file
            try:
                df = pd.read_csv(file_path, header=None, names=['Pattern', 'CCStrategy', 'Throughput'])
                data[pattern][strategy] = df['Throughput'].iloc[0]
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")
                data[pattern][strategy] = None

    # Plotting the data
    fig, axs = plt.subplots(1, 4, figsize=(20, 5), sharey=True)
    fig.suptitle('Throughput Comparison Across Different CC Strategies for Each Pattern')

    for i, (pattern, strategies) in enumerate(data.items()):
        strategies_names = list(strategies.keys())
        throughputs = list(strategies.values())

        axs[i].bar(strategies_names, throughputs, color='blue')
        axs[i].set_title(f'Throughput for {pattern}')
        axs[i].set_xlabel('CC Strategy')
        axs[i].set_ylabel('Throughput (requests/second)')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    # Save the figure in the same directory as the script
    script_dir = os.path.dirname(__file__)  # Get the directory where the script is located
    plt.savefig(os.path.join(script_dir, 'throughput_comparison_figure.png'))  # Save the figure

    plt.show()  # Show the figure

if __name__ == '__main__':
    root_directory = "/home/shuhao/DB4NFV/morphStream/scripts/nfvWorkload/experiments/pre_study"
    read_and_plot(root_directory)
