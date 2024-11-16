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

COLOR_MAP = ['#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F']
PATTERNS = ["\\", "///", "o", "||", "\\\\"]

# Fetch environment variables
project_Dir = os.environ.get("project_Dir", "/default/path/to/project")

# Paths
root_path = os.path.join(project_Dir, "scripts/ModernHardware/MicroArchitecturalAnalysis/")
FIGURE_FOLDER = os.path.join(project_Dir, "result.example/figures")
print(root_path)
print(FIGURE_FOLDER)


# Convert EPS to PDF
def ConvertEpsToPdf(dir_filename):
    os.system(f"epstopdf --outfile {dir_filename}.pdf {dir_filename}.eps")
    os.system(f"rm -rf {dir_filename}.eps")


# Draw bar chart figure
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    fig = plt.figure(figsize=(12, 6))  # Adjust size for horizontal orientation
    figure = fig.add_subplot(111)

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # Reverse the x_values and corresponding y_values for reversed order
    x_values = x_values[::-1]
    y_values = [row[::-1] for row in y_values]

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



# Read the CSV file and extract required metrics
def ReadCsvMetrics(file_path):
    metrics = {
        "Clockticks": 0,
        "Front-End Bound": 0.0,
        "Memory Bound": 0.0,
        "Core Bound": 0.0,
        "Bad Speculation": 0.0,
        "Retiring": 0.0,
    }
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for line in lines:
                parts = line.strip().split("\t")
                if len(parts) >= 2 and parts[1] in metrics:
                    try:
                        metrics[parts[1]] = float(parts[2])
                    except ValueError:
                        metrics[parts[1]] = 0.0  # Default to 0 if value conversion fails
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error while reading file {file_path}: {e}")
    return metrics


# Main entry point for reading and plotting
if __name__ == "__main__":
    # File paths
    file_paths = [
        os.path.join(root_path, "topdown_sstore.csv"),
        os.path.join(root_path, "topdown_tstream.csv"),
        os.path.join(root_path, "topdown_morphstream.csv"),
    ]

    x_values = ['S-Store', 'TStream', 'MorphStream']
    legend_labels = ["Frontend Bound", "Memory Bound", "Core Bound", "Bad Speculation", "Retiring"]

    y_values = []
    for file_path in file_paths:
        metrics = ReadCsvMetrics(file_path)
        print(metrics)
        y_values.append([
            metrics["Front-End Bound"] * metrics["Clockticks"] / 100,
            metrics["Memory Bound"] * metrics["Clockticks"] / 100,
            metrics["Core Bound"] * metrics["Clockticks"] / 100,
            metrics["Bad Speculation"] * metrics["Clockticks"] / 100,
            metrics["Retiring"] * metrics["Clockticks"] / 100,
        ])

    y_values = np.array(y_values).T.tolist()  # Transpose to match the bar chart structure
    DrawFigure(x_values, y_values, legend_labels, 'ClockTicks ($x10^{12}$)', '', 'topdown_breakdown', True)
