import argparse
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import os

# Load the CSV file to inspect its content
input_csv = ''

# Display the first few rows of the dataframe to understand its structure
# df.head()

# Number of Cores (n) - can be modified by the user
n = 7

def draw_packet_plot():
	# Performance graph.
	df = pd.read_csv(input_csv)

	# Convert timestamp to relative time in seconds
	df['ts'] = pd.to_numeric(df['ts'])
	start_time = df['ts'].iloc[0]

	df['Relative Time (s)'] = (df['ts'] - start_time) / 1e9

	# Columns related to packets
	packet_columns = ['num_pkts_done', 'num_alloc_pkts', 'num_deallocated_pkts', 'num_wip_packets']

	# Set the plot style
	sns.set(style="whitegrid")

	# Create a figure and a set of subplots
	fig, ax = plt.subplots(figsize=(12, 8))

	# Different line styles for each packet column
	line_styles = ['-', '--', '-.', ':']

	# Different colors for each core
	palette = sns.color_palette("hsv", n)

	# Plotting the data
	for i, column in enumerate(packet_columns):
		for core_id in range(n):
			core_data = df[df['CoreId'] == core_id]
			sns.lineplot(
				x='Relative Time (s)', 
				y=column, 
				data=core_data, 
				ax=ax, 
				color=palette[core_id], 
				linestyle=line_styles[i],
				label=f'Core {core_id} - {column}' if core_id == 0 else None
			)

	# Setting the plot title and labels
	ax.set_title('Packet Metrics Over Time for Each Core')
	ax.set_xlabel('Relative Time (s)')
	ax.set_ylabel('Packet Counts')

	# Adding a legend
	ax.legend(loc='upper left', bbox_to_anchor=(1, 1))

	# Display the plot
	plt.tight_layout()
	plt.savefig(os.path.join( output_dir ,'packets.png'))

def draw_latency_plot(input_csv, result_file_path, n=7):
    # Read the CSV file
    df = pd.read_csv(input_csv)

    # Convert timestamp to relative time in seconds
    df['ts'] = pd.to_numeric(df['ts'])
    start_time = df['ts'].iloc[0]
    df['Relative Time (s)'] = (df['ts'] - start_time) / 1e9

    # Latency columns and their corresponding names
    latency_columns = ['L0', 'L1', 'L2', 'L3']
    latency_names = {
        'L0': 'Latency to __request Issued',
        'L1': 'Latency to execute_sa_udf',
        'L2': 'Latency to sa_udf Done',
        'L3': 'Latency to handle_done'
    }

    # Create a figure with 5 subplots (1 for combined latency metrics and 4 for individual latencies)
    fig, axes = plt.subplots(5, 1, figsize=(12, 20))

    # Different colors for each latency value
    colors = ['blue', 'orange', 'green', 'red']

    # Subplot 1: Line graph for average, max, and min of each latency type across different cores
    for i, column in enumerate(latency_columns):
        # Calculating average, max, and min for each latency type, excluding zeros
        df_non_zero = df[df[column] != 0]
        avg_latency = df_non_zero.groupby('Relative Time (s)')[column].mean()
        max_latency = df_non_zero.groupby('Relative Time (s)')[column].max()
        min_latency = df_non_zero.groupby('Relative Time (s)')[column].min()

        axes[0].plot(avg_latency, color=colors[i], label=f'Average {latency_names[column]}')
        axes[0].plot(max_latency, color=colors[i], linestyle='--', label=f'Max {latency_names[column]}')
        axes[0].plot(min_latency, color=colors[i], linestyle=':', label=f'Min {latency_names[column]}')

    axes[0].set_title('Average, Max, and Min Latencies Over Time')
    axes[0].set_ylabel('Latency')
    axes[0].legend()

    # Subplots 2-5: Latency of each value for different cores
    for i, column in enumerate(latency_columns):
        for core_id in range(n):
            core_data = df[df['CoreId'] == core_id]
            sns.lineplot(x='Relative Time (s)', y=column, data=core_data, ax=axes[i+1], label=f'Core {core_id}')

        axes[i+1].set_title(f'Change of {latency_names[column]} Over Time')
        axes[i+1].set_ylabel('Latency')
        axes[i+1].legend()

    # Layout adjustment
    plt.tight_layout()

    # Save the figure
    plt.savefig(result_file_path)

    # Optionally show the plot
    plt.show()

def plot_csv():
	# 1st Packet plot.
	draw_packet_plot()
	# 2nd Latency plot.
	draw_latency_plot(input_csv,os.path.join(output_dir, 'latency_graph.png'))
	return 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot lines from a CSV file.")
    parser.add_argument("input_file", help="Input CSV file name")
    parser.add_argument("output_file", help="Output graph file name")

    args = parser.parse_args()
    input_csv = args.input_file
    output_dir = args.output_file

    plot_csv()
