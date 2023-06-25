import getopt
import os
import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator, LogLocator, MaxNLocator
from numpy import double

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 24
LABEL_FONT_SIZE = 28
LEGEND_FONT_SIZE = 29
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "///", "o", "||", "\\\\", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 0.0
MARKER_FREQUENCY = 1000

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME

def plot_cdf_multiple(files, algorithm_labels):
    plt.figure(figsize=(11, 5))  # Create a new figure
    line_styles = ['-', '--', ':', '-.']  # Define line styles for each line

    for i, (filename, label) in enumerate(zip(files, algorithm_labels)):
        # Read the first 2500 lines of processing times from the text file
        with open(filename, 'r') as file:
            processing_times = [float(next(file).strip()) for _ in range(300)]

        # Compute the CDF
        sorted_times = np.sort(processing_times)
        cdf = np.arange(1, len(sorted_times) + 1) / len(sorted_times)

        # Plot the CDF with algorithm label and line style
        if i < 2:
            plt.plot(sorted_times, cdf, label=label, linestyle=line_styles[0], linewidth=3)
        else:
            plt.plot(sorted_times, cdf, label=label, linestyle=line_styles[1], linewidth=3)

    plt.xlabel('Processing Time (msec)', fontproperties=LABEL_FP)
    plt.ylabel('CDF of Requests', fontproperties=LABEL_FP)
    plt.grid(True)
    # plt.legend(prop=LEGEND_FP)
    # plt.legend(prop=LEGEND_FP,
    #        loc='upper center',
    #        ncol=2,
    #        #                     mode='expand',
    #        bbox_to_anchor=(0.5, 1.3), shadow=False,
    #        columnspacing=0.3,
    #        frameon=True, borderaxespad=0.0, handlelength=1.5,
    #        handletextpad=0.1,
    #        labelspacing=0.1)
    
    legend = plt.legend(prop=LEGEND_FP,
                    loc='upper center',
                    ncol=2,
                    bbox_to_anchor=(0.5, 1.3),
                    shadow=False,
                    columnspacing=0.5,
                    frameon=True,
                    borderaxespad=0.0,
                    handlelength=1.5,
                    handletextpad=0.1,
                    labelspacing=0.1)

    legend.get_frame().set_linewidth(2)
    legend.get_frame().set_edgecolor('black')

    plt.subplots_adjust(top=0.95, bottom=0.15)
    # plt.show()
    plt.savefig("." + "/" + "latency_cdf.pdf", bbox_inches='tight')

# Example usage
files = ['/Users/zhonghao/Downloads/LoadBalancer/OG_NS_A/threads = 24/totalEvents = 7200/7200_5_20_10_0_1_false_0_5_300.latency', 
         '/Users/zhonghao/Downloads/LoadBalancer/OG_NS_A/threads = 24/totalEvents = 7200/7200_5_20_10_0_1_false_0_60_300.latency', 
         '/Users/zhonghao/Downloads/LoadBalancer/PAT/threads = 24/totalEvents = 7200/7200_5_20_10_0_1_false_0_5_300.latency', 
         
         '/Users/zhonghao/Downloads/LoadBalancer/PAT/threads = 24/totalEvents = 7200/7200_5_20_10_0_1_false_0_60_300.latency']
algorithm_labels = ['TransNFV (5% new)', 'TransNFV (60% new)', 'CHC (5% new)', 'CHC (60% new)']  # Replace with the algorithm labels
plot_cdf_multiple(files, algorithm_labels)

