import getopt
import os
import sys
import random

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

FIGURE_FOLDER = './results/overview'
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

# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_50_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_50_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/12288_25_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_75_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/1228800_25_25_0_100_true_10000
# /home/shuhao/TStream/data/stats/StreamLedger/OPGSA/threads = 24/totalEvents = 245760/122880_25_50_0_100_true_10000

def ReadFileSL(x_axis, batchInterval, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic,
               complexity):
    # w, h = 8, len(x_axis)
    w, h = 3, len(x_axis)
    y = [[] for _ in range(w)]
    # deposit_ratio_range = [0, 25, 50, 75, 100]
    deposit_ratio_range = [25, 50, 75]
    key_skewness_range = [25, 50, 75]
    abort_ratio_range = [1, 10, 100]
    NUM_ITEMS_range = [12288, 122880, 1228800]
    random_setting = ["deposit_ratio", "key_skewness", "abort_ratio", "NUM_ITEMS"]
    for punctuation_interval in x_axis:
        new_deposit_ratio = deposit_ratio
        new_key_skewness = key_skewness
        new_abort_ratio = abort_ratio
        new_NUM_ITEMS = NUM_ITEMS
        # pick a setting from a random range
        setting = random.choice(random_setting)
        if setting == "deposit_ratio":
            new_deposit_ratio = random.choice(deposit_ratio_range)
        elif setting == "key_skewness":
            new_key_skewness = random.choice(key_skewness_range)
        elif setting == "abort_ratio":
            new_abort_ratio = random.choice(abort_ratio_range)
        elif setting == "NUM_ITEMS":
            new_NUM_ITEMS = random.choice(NUM_ITEMS_range)

        if isCyclic == "true":
            inputEvents = tthread * batchInterval
            op_gs_path = getPathSL("OPGSA", inputEvents, tthread, new_NUM_ITEMS, new_deposit_ratio, new_key_skewness, overlap_ratio,
                                   new_abort_ratio, isCyclic, complexity)
            print(op_gs_path)
            lines = open(op_gs_path).readlines()
            throughput = lines[0].split(": ")[1]
            y[0].append(float(throughput))
        elif isCyclic == "false":
            inputEvents = tthread * batchInterval
            op_gs_path = getPathSL("GSA", inputEvents, tthread, new_NUM_ITEMS, new_deposit_ratio, new_key_skewness, overlap_ratio,
                                   new_abort_ratio, isCyclic, complexity)
            lines = open(op_gs_path).readlines()
            throughput = lines[0].split(": ")[1]
            y[0].append(float(throughput))
        else:
            print("error")

        inputEvents = tthread * batchInterval
        op_dfs_path = getPathSL("TStream", inputEvents, tthread, new_NUM_ITEMS, new_deposit_ratio, new_key_skewness, overlap_ratio,
                               new_abort_ratio, isCyclic, complexity)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

        inputEvents = tthread * batchInterval
        op_dfs_path = getPathSL("PAT", inputEvents, tthread, new_NUM_ITEMS, new_deposit_ratio, new_key_skewness, overlap_ratio,
                               new_abort_ratio, isCyclic, complexity)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    print(y)

    return y


def ReadFileGS(x_axis, batchInterval, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic,
               complexity):
    # w, h = 8, len(x_axis)
    w, h = 3, len(x_axis)
    y = [[] for _ in range(w)]

    NUM_ACCESS_range = [2, 4, 6]
    key_skewness_range = [25, 50, 75]
    abort_ratio_range = [1, 10, 100]
    NUM_ITEMS_range = [12288, 122880, 1228800]
    random_setting = ["NUM_ACCESS", "key_skewness", "abort_ratio", "NUM_ITEMS"]
    for punctuation_interval in x_axis:
        new_NUM_ACCESS = NUM_ACCESS
        new_key_skewness = key_skewness
        new_abort_ratio = abort_ratio
        new_NUM_ITEMS = NUM_ITEMS
        # pick a setting from a random range
        setting = random.choice(random_setting)
        if setting == "NUM_ACCESS":
            new_NUM_ACCESS = random.choice(NUM_ACCESS_range)
        elif setting == "key_skewness":
            new_key_skewness = random.choice(key_skewness_range)
        elif setting == "abort_ratio":
            new_abort_ratio = random.choice(abort_ratio_range)
        elif setting == "NUM_ITEMS":
            new_NUM_ITEMS = random.choice(NUM_ITEMS_range)

        if isCyclic == "true":
            inputEvents = tthread * batchInterval
            op_gs_path = getPathGS("OPGSA", inputEvents, tthread, new_NUM_ITEMS, new_NUM_ACCESS, new_key_skewness, overlap_ratio,
                                    new_abort_ratio, isCyclic, complexity)
            print(op_gs_path)
            lines = open(op_gs_path).readlines()
            throughput = lines[0].split(": ")[1]
            y[0].append(float(throughput))
        elif isCyclic == "false":
            inputEvents = tthread * batchInterval
            op_gs_path = getPathGS("GSA", inputEvents, tthread, new_NUM_ITEMS, new_NUM_ACCESS, new_key_skewness, overlap_ratio,
                                    new_abort_ratio, isCyclic, complexity)
            lines = open(op_gs_path).readlines()
            throughput = lines[0].split(": ")[1]
            y[0].append(float(throughput))
        else:
            print("error")

        inputEvents = tthread * batchInterval
        op_dfs_path = getPathGS("TStream", inputEvents, tthread, new_NUM_ITEMS, new_NUM_ACCESS, new_key_skewness, overlap_ratio,
                                    new_abort_ratio, isCyclic, complexity)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[1].append(float(throughput))

        inputEvents = tthread * batchInterval
        op_dfs_path = getPathGS("PAT", inputEvents, tthread, new_NUM_ITEMS, new_NUM_ACCESS, new_key_skewness, overlap_ratio,
                                    new_abort_ratio, isCyclic, complexity)
        lines = open(op_dfs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[2].append(float(throughput))

    print(y)

    return y


def getPathSL(algo, inputEvents, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic,
              complexity):
    return FILE_FOLER + '/StreamLedger/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}' \
        .format(algo, tthread, inputEvents, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic,
                complexity)


def getPathGS(algo, inputEvents, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic,
              complexity):
    return FILE_FOLER + '/GrepSum/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}' \
        .format(algo, tthread, inputEvents, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic,
                complexity)


if __name__ == '__main__':
    tthread = 24
    NUM_ITEMS = 122880
    NUM_ACCESS = 10
    deposit_ratio = 25
    key_skewness = 25
    overlap_ratio = 0
    abort_ratio = 100
    batchInterval = 10240
    isCyclic = "true"
    complexity = 10000

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

    x_value = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # randomly pick up a workload setting for each punctuation interval
    legend_labels = ["MorphStream", "TStream", "S-Store"]
    x_axis = [x_value] * len(legend_labels)
    legend = True
    y_axis = ReadFileSL(x_value, batchInterval, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio,
                        isCyclic, complexity)
    DrawFigure(x_axis, y_axis, legend_labels, "punctuation iterations", "throughput(e/s)",
               "sl_overview_dynamic_throughput_b{}_{}_{}_{}_{}_{}_{}_{}"
               .format(NUM_ITEMS, batchInterval, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic,
                       complexity),
               legend)
    y_axis = ReadFileGS(x_value, batchInterval, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio,
                        isCyclic, complexity)
    DrawFigure(x_axis, y_axis, legend_labels, "punctuation iterations", "throughput(e/s)",
               "gs_overview_dynamic_throughput_b{}_{}_{}_{}_{}_{}_{}_{}"
               .format(NUM_ITEMS, batchInterval, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic,
                       complexity),
               legend)