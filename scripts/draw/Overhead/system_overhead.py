import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator

# Constants for figure styling
OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 20
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

COLOR_MAP = ['#8C1A1C', '#731C8B', '#084C87', '#F6B804', '#9B2E09', '#1D7C36']
PATTERNS = ["\\", "///", "o", "||", "\\\\", ".."]

# Fetch environment variables
project_Dir = os.environ.get("project_Dir", "/default/path/to/project")

# Paths
root_path = os.path.join(project_Dir, "result/data/SystemOverhead/stats/StreamLedger/")
FIGURE_FOLDER = os.path.join(project_Dir, "result/figures")

# Draw bar chart figure
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    fig = plt.figure(figsize=(12, 6))  # Adjust size for horizontal orientation
    figure = fig.add_subplot(111)

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # # Reverse the x_values and corresponding y_values for reversed order
    # x_values = x_values[::-1]
    # y_values = [row[::-1] for row in y_values]

    index = np.arange(len(x_values))
    height = 0.5  # Bar height for horizontal bars
    left_base = np.zeros(len(y_values[0]))  # Base for stacking bars horizontally

    bars = []
    for i in range(len(y_values)):
        bar = plt.barh(index, y_values[i], height, hatch=PATTERNS[i],
                       color=COLOR_MAP[i], label=legend_labels[i], left=left_base,
                       edgecolor='black', linewidth=3)
        bars.append(bar)
        left_base += y_values[i]

    if allow_legend:
        handles, labels = figure.get_legend_handles_labels()
        plt.legend(handles[::-1], labels[::-1], loc='upper center', prop=LEGEND_FP, ncol=3,
                   bbox_to_anchor=(0.5, 1.2), handletextpad=0.1, borderaxespad=0.0,
                   handlelength=1.8, labelspacing=0.3, columnspacing=0.3)

    # Customize ticks and grid
    plt.yticks(index, x_values, fontproperties=TICK_FP)
    plt.grid(axis='x', color='gray')
    figure.xaxis.set_major_locator(LinearLocator(6))
    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    plt.savefig(f"{FIGURE_FOLDER}/{filename}.pdf", bbox_inches='tight', format='pdf')


# Read the input data and calculate averages
def ReadMetrics(file_path, system_type):
    construct_time, explore_time, sync_time, abort_time, lock_time, useful_time = 0, 0, 0, 0, 0, 0
    thread_count = 0

    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
            if system_type == "MorphStream":
                for line in lines:
                    if line.startswith("thread_id"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) >= 7:
                        construct_time += float(parts[5])
                        explore_time += float(parts[1])
                        abort_time += float(parts[6])
                        useful_time += float(parts[3])
                        thread_count += 1
                construct_time /= thread_count
                explore_time /= thread_count
                abort_time /= thread_count
                useful_time /= thread_count
            elif system_type == "TStream":
                for line in lines:
                    if line.startswith("thread_id"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) >= 7:
                        construct_time += float(parts[5])
                        sync_time += float(parts[1])
                        abort_time += float(parts[6])
                        useful_time += float(parts[3])
                        thread_count += 1
                construct_time /= thread_count
                sync_time /= thread_count
                abort_time /= thread_count
                useful_time /= thread_count
            elif system_type == "S-Store":
                for line in lines:
                    if line.startswith("thread_id"):
                        continue
                    parts = line.strip().split("\t")
                    if len(parts) >= 4:
                        sync_time += float(parts[3])
                        lock_time += float(parts[4])
                        useful_time += float(parts[2])
                        thread_count += 1
                sync_time /= thread_count
                lock_time /= thread_count
                useful_time /= thread_count
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error while reading file {file_path}: {e}")
    
    return [construct_time, explore_time, sync_time, abort_time, lock_time, useful_time]


# Main entry point for reading and plotting
if __name__ == "__main__":
    # File paths
    file_paths = {
        "MorphStream": os.path.join(root_path, "OG_BFS_A/threads = 24/totalEvents = 3194880/491520_100_0_10_0_1_true_8000"),
        "TStream": os.path.join(root_path, "TStream/threads = 24/totalEvents = 3194880/491520_100_0_10_0_1_false_8000"),
        "S-Store": os.path.join(root_path, "PAT/threads = 24/totalEvents = 3194880/491520_100_0_10_0_1_false_8000"),
    }

    x_values = ["MorphStream", "TStream", "S-Store"]
    legend_labels = ["Construct Time", "Explore Time", "Sync Time", "Abort Time", "Lock Time", "Useful Time"]

    # Parse data
    morph_data = ReadMetrics(file_paths["MorphStream"], "MorphStream")
    tstream_data = ReadMetrics(file_paths["TStream"], "TStream")
    sstore_data = ReadMetrics(file_paths["S-Store"], "S-Store")

    # Combine all system data for plotting
    system_data = [morph_data, tstream_data, sstore_data]
    print(system_data)

    # Ensure data is transposed for horizontal bar chart
    y_values = np.array(system_data).T.tolist()

    # Draw the breakdown chart
    DrawFigure(x_values, y_values, legend_labels, 'Time Breakdown (ms)', '', 'Figure13_a', True)
