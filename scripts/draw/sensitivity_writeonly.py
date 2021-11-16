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


def ReadFileGS(x_axis, tthread, batchInterval, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic):
    w, h = 3, len(x_axis)
    y = [[] for _ in range(w)]


    for key_skewness in x_axis:
        events = tthread * batchInterval
        op_gs_path = getPathGS("OPGSA", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    # for key_skewness in x_axis:
    #     events = tthread * batchInterval
    #     op_gs_path = getPathGS("GSA", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic)
    #     lines = open(op_gs_path).readlines()
    #     throughput = lines[0].split(": ")[1]
    #     y[1].append(float(throughput))

    for key_skewness in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPathGS("TStream", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for key_skewness in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPathGS("PAT", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    print(y)

    return y


def ReadFileSL(x_axis, tthread, batchInterval, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic):
    w, h = 3, len(x_axis)
    y = [[] for _ in range(w)]

    for deposit_ratio in x_axis:
        events = tthread * batchInterval
        op_gs_path = getPathSL("OPGSA", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    # for deposit_ratio in x_axis:
    #     events = tthread * batchInterval
    #     op_gs_path = getPathSL("GSA", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic)
    #     lines = open(op_gs_path).readlines()
    #     throughput = lines[0].split(": ")[1]
    #     y[1].append(float(throughput))


    for deposit_ratio in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPathSL("TStream", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for deposit_ratio in x_axis:
        events = tthread * batchInterval
        op_dfs_path = getPathSL("PAT", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    print(y)

    return y


def getPathSL(algo, events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic):
    return FILE_FOLER + '/StreamLedger/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}'\
        .format(algo, tthread, events, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic)


def getPathGS(algo, events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic):
    return FILE_FOLER + '/GrepSum/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}'\
        .format(algo, tthread, events, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic)


if __name__ == '__main__':
    tthread = 24
    NUM_ITEMS = 115200
    NUM_ACCESS = 10
    deposit_ratio = 25
    key_skewness = 0
    overlap_ratio = 0
    abort_ratio = 0
    batchInterval = 10240
    isCyclic = "false"

    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:n:k:o:a:b:c:")
    except getopt.GetoptError:
        print("Error")

    for opt, arg in opts:
        if opt in ['-i']:
            NUM_ITEMS = int(arg)
        elif opt in ['-d']:
            deposit_ratio = int(arg)
        elif opt in ['-n']:
            NUM_ACCESS = int(arg)
        elif opt in ['-k']:
            key_skewness = int(arg)
        elif opt in ['-o']:
            overlap_ratio = int(arg)
        elif opt in ['-a']:
            abort_ratio = int(arg)
        elif opt in ['-b']:
            batchInterval = int(arg)
        elif opt in ['-c']:
            if int(arg) == 1:
                isCyclic = "true"
            else:
                isCyclic = "false"


    x_value = [0, 25, 50, 75, 100]
    legend_labels = ["$MorphStream$", "$TStream$", "$S-Store$"]
    # legend_labels = ["$GS_{OC}$", "$BFS_{OC}$", "$DFS_{OC}$", "$GS_{OP}$", "$BFS_{OP}$", "$DFS_{OP}$", "PAT"]
    x_axis = [x_value] * len(legend_labels)
    legend = True
    y_axis = ReadFileSL(x_value, tthread, batchInterval, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic)
    DrawFigure(x_axis, y_axis, legend_labels, "writeonly(%)", "throughput(e/s)", "sl_writeonly_throughput_t{}_b{}_{}_{}_{}_{}_{}_{}"
            .format(tthread, NUM_ITEMS, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic),
            legend)
    # legend_labels = ["$GSA_{OC}$", "$BFSA_{OC}$", "$DFSA_{OC}$", "$GSA_{OP}$", "$BFSA_{OP}$", "$DFSA_{OP}$"]
    # x_axis = [x_value] * len(legend_labels)
    # y_axis = ReadFileWithAbort(x_value, batchInterval, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio)
    # DrawFigure(x_axis, y_axis, legend_labels, "tthreads", "throughput(e/s)",
    #             "comparison_with_abort_b{}_{}_{}_{}_{}"
    #             .format(batchInterval, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio), legend)