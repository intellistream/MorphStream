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
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 26
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

FIGURE_FOLDER = './results'
FILE_FOLER = '/home/shuhao/TStream/data/stats'


def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# draw a line chart
def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 5))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    x_values = xvalues
    y_values = yvalues
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                               linewidth=LINE_WIDTH, marker=MARKERS[i], \
                               markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                                markeredgewidth=1, markeredgecolor='k',
                                markevery=3)
    plt.axvline(x = 50, color = 'b', label = 'axvline - full height')
    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=4,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.2), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.yscale('log')
    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)
    plt.ylim(10, 10000)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

# the average latency
def averageLatency(lines):
    # get all latency of all files, calculate the average
    totalLatency = 0
    count = 0
    for line in lines:
        if line.startswith('keygroup: '):
            if line.split(": ")[-1][:-1] != "NaN":
                totalLatency += float(line.split(": ")[-1][:-1])
                count += 1

    if count > 0:
        return totalLatency / count
    else:
        return 0

def ReadFile(type):
    w, h = 3, 5
    y = [[] for _ in range(h)]
    col1 = []  # each col has 3 elements
    col2 = []
    col3 = []
    col4 = []

    affected_tasks = 2
    i = 0
    interval = 10000
    runtime = 150
    source_p = 5
    for rate in [1000, 2000, 4000, 8000, 9000]:
        for parallelism in [5, 10, 20]:
            n_tuples = int(rate * parallelism * runtime / source_p)
            print('trisk-{}-{}-{}-{}-{}-{}'.format(type, interval, parallelism, runtime, n_tuples, affected_tasks))
            # ${reconfig_type}-${reconfig_interval}-${parallelism}-${runtime}-${n_tuples}-${affected_tasks}
            exp = FILE_FOLER + '/trisk-{}-{}-{}-{}-{}-{}'.format(type, interval, parallelism, runtime,n_tuples, affected_tasks)
            file_path = os.path.join(exp, "Splitter FlatMap-0.output")
            if os.path.isfile(file_path):
                y[i].append(averageLatency(open(file_path).readlines()))
            else:
                exp = FILE_FOLER + '/trisk-{}-{}-{}-{}-{}-{}'.format(type, interval, parallelism, runtime,n_tuples, 4)
                file_path = os.path.join(exp, "Splitter FlatMap-0.output")
                y[i].append(averageLatency(open(file_path).readlines()))
        i += 1

    return y

if __name__ == '__main__':
    x_axis, y_axis = ReadFile()
    legend_labels = ["Megaphone", "Trisk", "Flink"]
    # legend_labels = ["Flink"]
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "time(s)", "latency(ms)", "comparison_latency", legend)
