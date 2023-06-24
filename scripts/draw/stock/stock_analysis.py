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
        lines[i], = figure.plot(x_values[i][:100], y_values[i][:100], color=LINE_COLORS[i], \
                                linewidth=LINE_WIDTH, marker=MARKERS[i], \
                                markersize=MARKER_SIZE, label=FIGURE_LABEL[i],
                                markeredgewidth=1, markeredgecolor='k', markevery=2)
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


def ReadFile():
    x = [[], [], []]
    y = [[], [], []]
    file = "/home/myc/workspace/MorphStream/test"
    fp = open(file)
    lines = fp.readlines()
    for line in lines:
        if "++++++ Stock id:" in line:
            textArr = line.split(" ")
            stock_id = int(textArr[3])
            if stock_id == 1:
                turnover_rate = float(textArr[6])
                y[0].append(turnover_rate)
            if stock_id == 2:
                turnover_rate = float(textArr[6])
                y[1].append(turnover_rate)
            if stock_id == 3:
                turnover_rate = float(textArr[6])
                y[2].append(turnover_rate)

    max_length = 0
    for sub_y in y:
        if len(sub_y) > max_length:
            max_length = len(sub_y)

    # x_axis = [i for i in range(0, max_length)]
    x[0] = [i for i in range(0, len(y[0]))]
    x[1] = [i for i in range(0, len(y[1]))]
    x[2] = [i for i in range(0, len(y[2]))]

    print(x)
    return x, y
    # return y

if __name__ == '__main__':
    legend_labels = ["Stock_1", "Stock_2", "Stock_3"]
    legend = True
    x_axis, y_axis = ReadFile()
    DrawFigure(x_axis, y_axis, legend_labels, "Per Stock Analysis", "Turnover Rate (%)", "stock_analysis",
               legend)