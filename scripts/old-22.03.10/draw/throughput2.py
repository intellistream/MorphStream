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
MARKER_SIZE = 10.0
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
    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

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
                                markeredgewidth=1, markeredgecolor='k')
    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(lines,
                   FIGURE_LABEL,
                   prop=LEGEND_FP,
                   loc='upper center',
                   ncol=6,
                   #                     mode='expand',
                   bbox_to_anchor=(0.5, 1.2), shadow=False,
                   columnspacing=0.1,
                   frameon=True, borderaxespad=0.0, handlelength=1.5,
                   handletextpad=0.1,
                   labelspacing=0.1)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


def ReadFile(x_axis, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio):
    w, h = 7, len(x_axis)
    y = [[] for _ in range(w)]

    for tthread in x_axis:
        events = tthread * batchInterval
        gs_path = getPath("GS", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        bfs_path = getPath("BFS", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        dfs_path = getPath("DFS", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_gs_path = getPath("OPGS", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[3].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_bfs_path = getPath("OPBFS", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[4].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPath("OPDFS", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[5].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPath("PAT", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[6].append(float(throughput))

    print(y)

    return y


def ReadFileWithAbort(x_axis, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio):
    w, h = 6, len(x_axis)
    y = [[] for _ in range(w)]

    for tthread in x_axis:
        events = tthread * batchInterval
        gs_path = getPath("GSA", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        bfs_path = getPath("BFSA", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        dfs_path = getPath("DFSA", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_gs_path = getPath("OPGSA", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[3].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_bfs_path = getPath("OPBFSA", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[4].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPath("OPDFSA", events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[5].append(float(throughput))

    return y


def getPath(algo, events, tthread, deposit_ratio, key_skewness, overlap_ratio, abort_ratio):
    return FILE_FOLER + '/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}'\
        .format(algo, tthread, events, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)


if __name__ == '__main__':
    deposit_ratio = 0
    key_skewness = 0
    overlap_ratio = 10
    abort_ratio = 0
    batchInterval = 10240
    # for deposit_ratio in [0, 25, 50, 75, 100]:
    # for key_skewness in [0, 25, 50]:
    # for overlap_ratio in [0, 25, 50]:
    for tthread in [24, 48]:
        x_value = [1, 2, 4, 8, 16, 24]
        legend_labels = ["$GS_{OC}$", "$BFS_{OC}$", "$DFS_{OC}$", "$GS_{OP}$", "$BFS_{OP}$", "$DFS_{OP}$", "PAT"]
        x_axis = [x_value] * len(legend_labels)
        y_axis = ReadFile(x_value, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        legend = True
        DrawFigure(x_axis, y_axis, legend_labels, "tthreads", "throughput(e/s)", "comparison_b{}_{}_{}_{}_{}"
                    .format(batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio),
                    legend)

        legend_labels = ["$GSA_{OC}$", "$BFSA_{OC}$", "$DFSA_{OC}$", "$GSA_{OP}$", "$BFSA_{OP}$", "$DFSA_{OP}$"]
        x_axis = [x_value] * len(legend_labels)
        y_axis = ReadFileWithAbort(x_value, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio)
        DrawFigure(x_axis, y_axis, legend_labels, "tthreads", "throughput(e/s)",
                    "comparison_with_abort_b{}_{}_{}_{}_{}"
                    .format(batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio), legend)
