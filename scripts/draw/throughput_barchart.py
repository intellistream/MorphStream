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

FIGURE_FOLDER = './results'
FILE_FOLER = '/home/shuhao/TStream/data/stats'


def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# draw a bar chart
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.08
    # draw the bars
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        bars[i] = plt.bar(index + i * width + width / 2,
                          y_values[i], width,
                          hatch=PATTERNS[i],
                          color=LINE_COLORS[i],
                          label=FIGURE_LABEL[i], edgecolor='black', linewidth=3)

    # # sometimes you may not want to draw legends.
    # if allow_legend == True:
    #     plt.legend(bars, FIGURE_LABEL,
    #                prop=LEGEND_FP,
    #                ncol=5,
    #                loc='upper center',
    #                #                     mode='expand',
    #                shadow=False,
    #                bbox_to_anchor=(0.45, 1.6),
    #                columnspacing=0.1,
    #                handletextpad=0.2,
    #                #                     bbox_transform=ax.transAxes,
    #                #                     frameon=True,
    #                #                     columnspacing=5.5,
    #                #                     handlelength=2,
    #                )

    # you may need to tune the xticks position to get the best figure.
    plt.xticks(index + 2.5 * width, x_values)
    # plt.ylim(0, 100)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


def ReadFile(threads, events):
    w, h = 12, 1
    y = [[] for _ in range(h)]

    gs_path = FILE_FOLER + '/GS/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(gs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    bfs_path = FILE_FOLER + '/BFS/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(bfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    dfs_path = FILE_FOLER + '/DFS/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(dfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    op_gs_path = FILE_FOLER + '/OPGS/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(op_gs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    op_bfs_path = FILE_FOLER + '/OPBFS/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(op_bfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    op_dfs_path = FILE_FOLER + '/OPDFS/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(op_dfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    print(y)

    return y


def ReadFileWithAbort(threads, events):
    w, h = 12, 1
    y = [[] for _ in range(h)]

    gs_path = FILE_FOLER + '/GSA/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(gs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    bfs_path = FILE_FOLER + '/BFSA/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(bfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    dfs_path = FILE_FOLER + '/DFSA/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(dfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    op_gs_path = FILE_FOLER + '/OPGSA/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(op_gs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    op_bfs_path = FILE_FOLER + '/OPBFSA/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(op_bfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    op_dfs_path = FILE_FOLER + '/OPDFSA/threads = {}/totalEvents = {}'.format(threads, events)

    lines = open(op_dfs_path).readlines()
    throughput = lines[0].split(": ")[1]
    y[0].append(float(throughput))

    print(y)

    return y


if __name__ == '__main__':
    for tthread in [1, 2, 4, 8, 16, 24]:
        for batchInterval in [1024, 2048, 4096, 8192, 10240]:
            totalEvents = tthread * batchInterval
            y_values = ReadFile(tthread, totalEvents)
            x_values = ["$GS_{OC}$", "$BFS_{OC}$", "$DFS_{OC}$", "$GS_{OP}$", "$BFS_{OP}$", "$DFS_{OP}$"]
            legend_labels = ["throughput"]
            legend = True
            DrawFigure(x_values, y_values, legend_labels,
                       '', 'throughput', 'overview_t{}_b{}'.format(tthread, batchInterval), True)

            y_values = ReadFileWithAbort(tthread, totalEvents)
            x_values = ["$GSA_{OC}$", "$BFSA_{OC}$", "$DFSA_{OC}$", "$GSA_{OP}$", "$BFSA_{OP}$", "$DFSA_{OP}$"]
            legend_labels = ["throughput"]
            DrawFigure(x_values, y_values, legend_labels,
                       '', 'throughput', 'overview_with_abort_t{}_b{}'.format(tthread, batchInterval), True)
