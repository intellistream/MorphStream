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


def ReadFile(x_axis, batchInterval):
    w, h = 6, len(x_axis)
    y = [[] for _ in range(h)]

    for tthread in x_axis:
        events = tthread * batchInterval
        gs_path = FILE_FOLER + '/GS/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        bfs_path = FILE_FOLER + '/BFS/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        dfs_path = FILE_FOLER + '/DFS/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_gs_path = FILE_FOLER + '/OPGS/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[3].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_bfs_path = FILE_FOLER + '/OPBFS/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(op_bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[4].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_dfs_path = FILE_FOLER + '/OPDFS/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[5].append(float(throughput))

    print(y)

    return y

def ReadFileWithAbort(x_axis, batchInterval):
    w, h = 6, len(x_axis)
    y = [[] for _ in range(h)]

    for tthread in x_axis:
        events = tthread * batchInterval
        gs_path = FILE_FOLER + '/GSA/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        bfs_path = FILE_FOLER + '/BFSA/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        dfs_path = FILE_FOLER + '/DFSA/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_gs_path = FILE_FOLER + '/OPGSA/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[3].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_bfs_path = FILE_FOLER + '/OPBFSA/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(op_bfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[4].append(float(throughput))

    for tthread in x_axis:
        events = tthread * batchInterval
        op_dfs_path = FILE_FOLER + '/OPDFSA/threads = {}/totalEvents = {}'.format(tthread, events)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[5].append(float(throughput))

    return y

if __name__ == '__main__':
    batchInterval = 4096
    x_value = [1, 2, 4, 8, 16, 24]
    legend_labels = ["GS", "BFS", "DFS", "OPGS", "OPBFS", "OPDFS"]
    x_axis = [x_value] * len(legend_labels)
    y_axis = ReadFile(x_value, batchInterval)
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "tthreads", "throughput(e/s)", "comparison", legend)

    legend_labels = ["GSA", "BFSA", "DFSA", "OPGSA", "OPBFSA", "OPDFSA"]
    x_axis = [x_value] * len(legend_labels)
    y_axis = ReadFileWithAbort(x_value, batchInterval)
    DrawFigure(x_axis, y_axis, legend_labels, "tthreads", "throughput(e/s)", "comparison_with_abort", legend)
