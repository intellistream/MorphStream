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

MARKERS = (["x", 'o', 's', "^", "h", "v", ">", "<", "d", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#B03A2E', '#2874A6', '#239B56', '#7D3C98', '#F1C40F', '#F5CBA7', '#82E0AA', '#AEB6BF', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "///", "o", "||", "\\\\", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
LABEL_WEIGHT = 'bold'
LINE_COLORS = COLOR_MAP
LINE_WIDTH = 3.0
MARKER_SIZE = 20.0
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
                                markeredgewidth=1, markeredgecolor='k', markevery=int(len(x_values[i])/15),  mfc='none')
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
    # plt.xlim(right=1000)

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight')


def ReadFile():
    interval = 1000000
    truth_list = []
    actual_list = []
    event_id_list = []
    truth_cdf = {}
    actual_cdf = {}
    x = [[], []]
    y = [[], []]

    source_file = "/home/myc/workspace/MorphStream-Stock/application/src/main/java/benchmark/datagenerator/apps/SHJ/dataset/stock_dataset_v2.csv"
    fp = open(source_file)
    event_ts_offset = {}
    inputEvents = fp.readlines()
    for inputEvent in inputEvents:
        textArr = inputEvent.split(",")
        event_ts_offset[int(textArr[0])] = int(textArr[1])

    file = "/home/myc/workspace/MorphStream-Stock/test"
    fp = open(file)
    lines = fp.readlines()
    fp.close()
    for line in lines:
        if "++++++ Completed:" in line:
            textArr = line.split(" ")
            actual_list.append(int(textArr[2]))
            truth_list.append(int(textArr[4]))
            event_id_list.append(int(textArr[6]))


    start_ts = truth_list[0]

    for i in range(0, len(truth_list)):
        event_offset = event_ts_offset[event_id_list[i]]
        print(event_offset)
        truth_index = int((truth_list[i] - start_ts) / interval) + event_offset
        actual_index = int((actual_list[i] - start_ts) / interval) + event_offset
        # truth_index = int((truth_list[i] - start_ts) / interval) + event_offset
        # actual_index = int((actual_list[i] - start_ts) / interval) + event_ts_offset[39996]
        # truth_index = int((truth_list[i] - start_ts) / interval)
        # actual_index = int((actual_list[i] - start_ts) / interval)
        if truth_index not in truth_cdf:
            truth_cdf[truth_index] = 0
        truth_cdf[truth_index] += 1
        if actual_index not in actual_cdf:
            actual_cdf[actual_index] = 0
        actual_cdf[actual_index] += 1

    # start_ts = truth_list[0]
    # for ts in truth_list:
    #     index = int((ts - start_ts) / interval)
    #     if index not in truth_cdf:
    #         truth_cdf[index] = 0
    #     truth_cdf[index] += 1
    #
    # # start_ts = actual_list[0]
    # for ts in actual_list:
    #     index = int((ts - start_ts) / interval)
    #     if index not in actual_cdf:
    #         actual_cdf[index] = 0
    #     actual_cdf[index] += 1

    x[0] = list(truth_cdf.keys())
    x[1] = list(actual_cdf.keys())

    sum = 0
    for key in truth_cdf:
        sum += truth_cdf[key]
        y[0].append(sum)

    sum = 0
    for key in actual_cdf:
        sum += actual_cdf[key]
        y[1].append(sum)

    return x, y
    # return y

if __name__ == '__main__':
    legend_labels = ["Expected-Output", "Actual-Output"]
    legend = True
    x_axis, y_axis = ReadFile()
    DrawFigure(x_axis, y_axis, legend_labels, "Time (ms)", "Accumulated Matched Results", "stock_performance",
               legend)