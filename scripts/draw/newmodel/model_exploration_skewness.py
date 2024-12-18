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
MARKER_SIZE = 10.0
MARKER_FREQUENCY = 1000

matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
matplotlib.rcParams['xtick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['ytick.labelsize'] = TICK_FONT_SIZE
matplotlib.rcParams['font.family'] = OPT_FONT_NAME

FIGURE_FOLDER = './results/sensitivity'
FILE_FOLER = '/home/shuhao/jjzhao/data/stats'


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


def ReadFileGS(x_axis, tthread, batchInterval, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    w, h = 3, len(x_axis)
    y = [[] for _ in range(w)]


    for key_skewness in x_axis:
        inputEvents = tthread * batchInterval
        op_gs_path = getPathGS("OP_NS", inputEvents, tthread, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for key_skewness in x_axis:
        inputEvents = tthread * batchInterval
        op_dfs_path = getPathGS("OP_BFS", inputEvents, tthread, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    for key_skewness in x_axis:
        inputEvents = tthread * batchInterval
        op_dfs_path = getPathGS("OP_DFS", inputEvents, tthread, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    print(y)

    return y



def getPathGS(algo, inputEvents, tthread, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    return FILE_FOLER + '/GrepSum/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}_{}'\
        .format(algo, tthread, inputEvents, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio,1, isCyclic, complexity)


if __name__ == '__main__':
    tthread = 24
    NUM_ITEMS = 115200
    NUM_ACCESS = 1
    Ratio_of_Multiple_State_Access = 100
    key_skewness = 0
    overlap_ratio = 0
    abort_ratio = 0
    batchInterval = 10240
    isCyclic = "false"
    complexity = 1000

    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:n:k:o:a:b:c:m:")
    except getopt.GetoptError:
        print("Error")

    for opt, arg in opts:
        if opt in ['-i']:
            NUM_ITEMS = int(arg)
        elif opt in ['-d']:
            Ratio_of_Multiple_State_Access = int(arg)
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
        elif opt in ['-m']:
            complexity = int(arg)


    x_value = [0, 25, 50, 75, 100]
    legend_labels = ["NS", "BFS" ,"DFS"]
    legend = True

    y_axis = ReadFileGS(x_value, tthread, batchInterval, NUM_ITEMS, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
    DrawFigure(x_axis, y_axis, legend_labels, "zipf_skew", "Throughput (K/sec)", "gs_skewness_throughput_t{}_b{}_{}_{}_{}_{}_{}_{}_{}"
                .format(tthread, NUM_ITEMS, batchInterval, Ratio_of_Multiple_State_Access, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity),
                legend)