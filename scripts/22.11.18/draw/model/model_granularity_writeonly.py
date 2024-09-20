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

FIGURE_FOLDER = './results/model/granularity'
FILE_FOLER = '/home/shuhao/data/stats'


def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# draw a bar chart
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, y_min, y_max, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 5))
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
                          label=FIGURE_LABEL[i],
                          edgecolor="black", lw=3
                          )

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL,
                   prop=LEGEND_FP,
                   ncol=4,
                   loc='upper center',
                   # mode='expand',
                   shadow=False,
                   bbox_to_anchor=(0.5, 1.2),
                   columnspacing=0.1,
                   handletextpad=0.2,
                   #                     bbox_transform=ax.transAxes,
                   #                     frameon=True,
                   #                     columnspacing=5.5,
                   handlelength=2,
                   )

    plt.xticks(index + 2 * width, x_values)
    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')

def ReadFileSL(x_axis, tthread, batchInterval, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity):
    w, h = 2, len(x_axis)
    y = [[] for _ in range(w)]

    for deposit_ratio in x_axis:
        events = tthread * batchInterval
        op_gs_path = getPathSL("OP_NS_A", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    for deposit_ratio in x_axis:
        events = tthread * batchInterval
        op_gs_path = getPathSL("OG_NS_A", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

    print(y)

    return y

def getPathSL(algo, events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity):
    return FILE_FOLER + '/StreamLedger/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}_{}'\
        .format(algo, tthread, events, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity)


def getPathGS(algo, events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity):
    return FILE_FOLER + '/GrepSum/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}_{}'\
        .format(algo, tthread, events, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity)


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
    complexity = 1000
    txn_length = 1

    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:n:k:o:a:b:c:m:")
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
        elif opt in ['-m']:
            complexity = int(arg)

    # NUM_ACCESS
    x_values = [0, 25, 50, 75, 100]
    legend_labels = ["Fine-grained", "Coarse-grained"]
    legend = True
    y_values = ReadFileSL(x_values, tthread, batchInterval, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio,
                          abort_ratio, txn_length, isCyclic, complexity)
    DrawFigure(x_values, y_values, legend_labels,
               'Ratio of Writeonly Txns', 'Throughput (K/sec)', 0,
               400, 'sl_granularity_comparison_writeonly_t{}_b{}_{}_{}_{}_{}_{}_{}_{}_{}'
                .format(tthread, NUM_ITEMS, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, txn_length, isCyclic, complexity), legend)
