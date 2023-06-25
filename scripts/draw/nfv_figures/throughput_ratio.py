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
LEGEND_FONT_SIZE = 30
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

# Throughput data for three algorithms
# TransNFV_throughput = [109, 86, 78, 50, 46]
# PAT_throughput = [59, 36, 28, 29, 25]
# NOCC_throughput = [257, 244, 221, 191, 186]

TransNFV_throughput = [102, 89, 77, 64, 51]
PAT_throughput = [49, 39, 31, 28, 22]
NOCC_throughput = [252, 238, 219, 196, 186]

# Ratio of connections from 10 to 100
ratios = [5, 10, 20, 40, 60]

# Creating bar positions for each ratio
bar_positions = np.arange(len(ratios))

# Bar width
bar_width = 0.3

# fig = plt.figure(figsize=(10, 6))
fig, ax = plt.subplots(figsize=(10, 5))
# Plotting the bar charts
# plt.bar(bar_positions - bar_width, NOCC_throughput, width=bar_width, hatch="\\", color='#F1C40F', label='No Sharing')
# Plotting the line chart for NOCC_throughput
ax.plot(bar_positions, NOCC_throughput, marker='o', color='black', 
        linewidth=3, markersize=MARKER_SIZE, label='No Sharing')
plt.bar(bar_positions - 0.5*bar_width, TransNFV_throughput, 
        width=bar_width, hatch="//", color='#F1C40F', 
        label='TransNFV', edgecolor='black', linewidth=2)
# plt.bar(bar_positions - 0.5*bar_width, TransNFV_throughput, 
#         width=bar_width, hatch="//", color='#AEB6BF', 
#         label='TransNFV', edgecolor='black', linewidth=2)
plt.bar(bar_positions + 0.5*bar_width, PAT_throughput, 
        width=bar_width, hatch="\\", color='#2874A6', 
        label='CHC', edgecolor='black', linewidth=2)

# plt.bar(bar_positions + bar_width, algorithm3_throughput, width=bar_width, label='Algorithm 3')

# Adding labels and title
plt.xlabel('Ratio of New Connections (%)', fontproperties=LABEL_FP)
plt.ylabel('Throughput (K req/sec)', fontproperties=LABEL_FP)
# plt.title('Throughput Comparison for Different Algorithms')



# Adding x-axis tick labels
plt.xticks(bar_positions, ratios)

# Adding legend
# plt.legend()
# plt.legend(prop=LEGEND_FP,
#            loc='upper center',
#            ncol=3,
#            #                     mode='expand',
#            bbox_to_anchor=(0.5, 1.2), shadow=False,
#            columnspacing=0.5,
#            frameon=True, borderaxespad=0.0, handlelength=1.5,
#            handletextpad=0.1,
#            labelspacing=0.1)

legend = plt.legend(prop=LEGEND_FP,
                    loc='upper center',
                    ncol=3,
                    bbox_to_anchor=(0.5, 1.23),
                    shadow=False,
                    columnspacing=0.5,
                    frameon=True,
                    borderaxespad=0.0,
                    handlelength=1.5,
                    handletextpad=0.1,
                    labelspacing=0.1)

legend.get_frame().set_linewidth(2)
legend.get_frame().set_edgecolor('black')


plt.grid()

# Displaying the plot
# plt.show()

# DrawFigure(ratios, TransNFV_throughput, ratios, '', 'throughput', True)
plt.savefig("." + "/" + "throughput_ratio.pdf", bbox_inches='tight')
