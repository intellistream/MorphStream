import getopt
import os
import sys
from math import ceil

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator, LogLocator, MaxNLocator, PercentFormatter
from numpy import double
from numpy.ma import arange

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
def DrawFigure(xvalues, yvalues, legend_labels, x_label, y_label, x_min, x_max, y_min, y_max, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(10, 5))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    x_values = xvalues
    y_values = yvalues
    print("mark gap:", ceil(x_max / 6))
    lines = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        lines[i], = figure.plot(x_values[i], y_values[i], color=LINE_COLORS[i], \
                           linewidth=LINE_WIDTH, marker=MARKERS[i], \
                           markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                           markevery=ceil(x_max / 6), markeredgewidth=2, markeredgecolor='k'
                           )

    # # sometimes you may not want to draw legends.
    # if allow_legend == True:
    #     plt.legend(lines,
    #                FIGURE_LABEL,
    #                prop=LEGEND_FP,
    #                loc='upper center',
    #                ncol=4,
    #                #                     mode='expand',
    #                bbox_to_anchor=(0.55, 1.5), shadow=False,
    #                columnspacing=0.1,
    #                frameon=True, borderaxespad=0.0, handlelength=1.5,
    #                handletextpad=0.1,
    #                labelspacing=0.1)

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

    # plt.xscale('log')
    # plt.xticks(x_values)
    # you may control the limits on your own.
    plt.xlim(left=0)
    # plt.ylim(y_min, y_max)
    plt.grid(axis='y', color='gray')

    figure.yaxis.set_major_formatter(PercentFormatter(1.0))
    # figure.yaxis.set_major_locator(LogLocator(base=10))
    # figure.xaxis.set_major_locator(matplotlib.ticker.FixedFormatter(["0.25", "0.5", "0.75", "1"]))
    # figure.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0))
    # figure.xaxis.set_major_locator(pylab.LinearLocator(6))
    # figure.xaxis.set_major_locator(LogLocator(base=10))
    # figure.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    # size = fig.get_size_inches()
    # dpi = fig.get_dpi()

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')
    # ConvertEpsToPdf(FIGURE_FOLDER + "/" + filename)


def ReadFileSL(batchInterval, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    x_axis = []
    y_axis = []
    events = tthread * batchInterval

    if isCyclic == "true":
        col = []
        f = getPathSL("OPGSA", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
        lines = open(f).readlines()
        count = 0
        for line in lines:
            count += 1
            value = double(line.strip("\n"))  # timestamp.
            col.append(value)
            if count == 200:
                break
        # calculate the proportional values of samples
        coly = 1. * arange(len(col)) / (len(col) - 1)
        x_axis.append(col)
        y_axis.append(coly)
    elif isCyclic == "false":
        col = []
        f = getPathSL("GSA", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
        lines = open(f).readlines()
        count = 0
        for line in lines:
            count += 1
            value = double(line.strip("\n"))  # timestamp.
            col.append(value)
            if count == 200:
                break
        # calculate the proportional values of samples
        coly = 1. * arange(len(col)) / (len(col) - 1)
        x_axis.append(col)
        y_axis.append(coly)
    else:
        print ("error")

    col = []
    f = getPathSL("TStream", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
    lines = open(f).readlines()
    count = 0
    for line in lines:
        count += 1
        value = double(line.strip("\n"))  # timestamp.
        col.append(value)
        if count == 200:
            break
    # calculate the proportional values of samples
    coly = 1. * arange(len(col)) / (len(col) - 1)
    x_axis.append(col)
    y_axis.append(coly)

    col = []
    f = getPathSL("PAT", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio,
                  isCyclic, complexity)
    lines = open(f).readlines()
    count = 0
    for line in lines:
        count += 1
        value = double(line.strip("\n"))  # timestamp.
        col.append(value)
        if count == 200:
            break
    # calculate the proportional values of samples
    coly = 1. * arange(len(col)) / (len(col) - 1)
    x_axis.append(col)
    y_axis.append(coly)

    return x_axis, y_axis



def ReadFileGS(batchInterval, tthread, NUM_ITEMS, NUM_ACCESS,  key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    x_axis = []
    y_axis = []
    events = tthread * batchInterval

    if isCyclic == "true":
        col = []
        f = getPathGS("OPGSA", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio,
                    isCyclic, complexity)
        lines = open(f).readlines()
        count = 0
        for line in lines:
            count += 1
            value = double(line.strip("\n"))  # timestamp.
            col.append(value)
            if count == 200:
                break
        # calculate the proportional values of samples
        coly = 1. * arange(len(col)) / (len(col) - 1)
        x_axis.append(col)
        y_axis.append(coly)
    elif isCyclic == "false":
        col = []
        f = getPathGS("GSA", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
        lines = open(f).readlines()
        count = 0
        for line in lines:
            count += 1
            value = double(line.strip("\n"))  # timestamp.
            col.append(value)
            if count == 200:
                break
        # calculate the proportional values of samples
        coly = 1. * arange(len(col)) / (len(col) - 1)
        x_axis.append(col)
        y_axis.append(coly)
    else:
        print ("error")

    col = []
    f = getPathGS("TStream", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio,
                  isCyclic, complexity)
    lines = open(f).readlines()
    count = 0
    for line in lines:
        count += 1
        value = double(line.strip("\n"))  # timestamp.
        col.append(value)
        if count == 200:
            break
    # calculate the proportional values of samples
    coly = 1. * arange(len(col)) / (len(col) - 1)
    x_axis.append(col)
    y_axis.append(coly)

    col = []
    f = getPathGS("PAT", events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio,
                  isCyclic, complexity)
    lines = open(f).readlines()
    count = 0
    for line in lines:
        count += 1
        value = double(line.strip("\n"))  # timestamp.
        col.append(value)
        if count == 200:
            break
    # calculate the proportional values of samples
    coly = 1. * arange(len(col)) / (len(col) - 1)
    x_axis.append(col)
    y_axis.append(coly)

    return x_axis, y_axis


def getPathSL(algo, events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    return FILE_FOLER + '/StreamLedger/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}.latency'\
        .format(algo, tthread, events, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)


def getPathGS(algo, events, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    return FILE_FOLER + '/GrepSum/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}.latency'\
        .format(algo, tthread, events, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)

def getCountSL(events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    file = getPathSL("OPGSA", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
    f = open(file, "r")
    read = f.readlines()
    return len(read)

def getCountGS(events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity):
    file = getPathGS("OPGSA", events, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
    f = open(file, "r")
    read = f.readlines()
    return len(read)

if __name__ == '__main__':
    tthread=24
    NUM_ITEMS = 115200
    NUM_ACCESS=10
    deposit_ratio = 25
    key_skewness = 25
    overlap_ratio = 0
    abort_ratio = 100
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

    # legend_labels = ["$GS_{OC}$", "$BFS_{OC}$", "$DFS_{OC}$", "$GS_{OP}$", "$BFS_{OP}$", "$DFS_{OP}$", "TStream", "PAT"]
    legend_labels = ["$MorphStream$", "$TStream$", "$S-Store$"]
    x_axis, y_axis = ReadFileSL(batchInterval, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
    legend = True
    DrawFigure(x_axis, y_axis, legend_labels, "latency(ms)", "cumulative percent",
               0, getCountSL(batchInterval * tthread, tthread, NUM_ITEMS, deposit_ratio, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity),
               1, 0,
               "sl_overview_latency_b{}_{}_{}_{}_{}_{}_{}_{}".format(NUM_ITEMS, batchInterval, deposit_ratio, key_skewness,
                                                                  overlap_ratio, abort_ratio, isCyclic, complexity),
               legend)
    x_axis, y_axis = ReadFileGS(batchInterval, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio, abort_ratio, isCyclic, complexity)
    DrawFigure(x_axis, y_axis, legend_labels, "latency(ms)", "cumulative percent",
               0, getCountGS(batchInterval * tthread, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, overlap_ratio,
                             abort_ratio, isCyclic, complexity),
               1, 0,
               "gs_overview_latency_b{}_{}_{}_{}_{}_{}_{}_{}".format(NUM_ITEMS, batchInterval, NUM_ACCESS, key_skewness,
                                                                  overlap_ratio, abort_ratio, isCyclic, complexity),
               legend)