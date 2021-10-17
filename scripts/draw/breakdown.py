import getopt
import os
import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import LinearLocator
from numpy import double

OPT_FONT_NAME = 'Helvetica'
TICK_FONT_SIZE = 20
LABEL_FONT_SIZE = 24
LEGEND_FONT_SIZE = 20
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
matplotlib.rcParams['pdf.fonttype'] = 42

FIGURE_FOLDER = './results/breakdown'
FILE_FOLER = '/home/shuhao/TStream/data/stats'


# there are some embedding problems if directly exporting the pdf figure using matplotlib.
# so we generate the eps format first and convert it to pdf.
def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


# draw a line chart
def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
    # you may change the figure size on your own.
    fig = plt.figure(figsize=(12, 3))
    figure = fig.add_subplot(111)

    FIGURE_LABEL = legend_labels

    if not os.path.exists(FIGURE_FOLDER):
        os.makedirs(FIGURE_FOLDER)

    # values in the x_xis
    index = np.arange(len(x_values))
    # the bar width.
    # you may need to tune it to get the best figure.
    width = 0.5
    # draw the bars
    bottom_base = np.zeros(len(y_values[0]))
    bars = [None] * (len(FIGURE_LABEL))
    for i in range(len(y_values)):
        bars[i] = plt.bar(index + width / 2, y_values[i], width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                          label=FIGURE_LABEL[i], bottom=bottom_base, edgecolor='black', linewidth=3)
        bottom_base = np.array(y_values[i]) + bottom_base

    # sometimes you may not want to draw legends.
    if allow_legend == True:
        plt.legend(bars, FIGURE_LABEL
                   #                     mode='expand',
                   #                     shadow=False,
                   #                     columnspacing=0.25,
                   #                     labelspacing=-2.2,
                   #                     borderpad=5,
                   #                     bbox_transform=ax.transAxes,
                   #                     frameon=False,
                   #                     columnspacing=5.5,
                   #                     handlelength=2,
                   )
        if allow_legend == True:
            handles, labels = figure.get_legend_handles_labels()
        if allow_legend == True:
            print(handles[::-1], labels[::-1])
            leg = plt.legend(handles[::-1], labels[::-1],
                             loc='center',
                             prop=LEGEND_FP,
                             ncol=3,
                             bbox_to_anchor=(0.5, 1.3),
                             handletextpad=0.1,
                             borderaxespad=0.0,
                             handlelength=1.8,
                             labelspacing=0.3,
                             columnspacing=0.3,
                             )
            leg.get_frame().set_linewidth(2)
            leg.get_frame().set_edgecolor("black")

    plt.ylim(0, 100)

    # you may need to tune the xticks position to get the best figure.
    plt.xticks(index + 0.5 * width, x_values)
    plt.xticks(rotation=20)

    plt.grid(axis='y', color='gray')
    figure.yaxis.set_major_locator(LinearLocator(6))

    figure.get_xaxis().set_tick_params(direction='in', pad=10)
    figure.get_yaxis().set_tick_params(direction='in', pad=10)

    plt.xlabel(x_label, fontproperties=LABEL_FP)
    plt.ylabel(y_label, fontproperties=LABEL_FP)

    size = fig.get_size_inches()
    dpi = fig.get_dpi()

    plt.savefig(FIGURE_FOLDER + "/" + filename + ".pdf", bbox_inches='tight', format='pdf')


def DrawLegend(legend_labels, filename):
    fig = pylab.figure()
    ax1 = fig.add_subplot(111)
    FIGURE_LABEL = legend_labels
    LEGEND_FP = FontProperties(style='normal', size=26)

    bars = [None] * (len(FIGURE_LABEL))
    data = [1]
    x_values = [1]

    width = 0.3
    for i in range(len(FIGURE_LABEL)):
        bars[i] = ax1.bar(x_values, data, width, hatch=PATTERNS[i], color=LINE_COLORS[i],
                          linewidth=0.2)

    # LEGEND
    figlegend = pylab.figure(figsize=(11, 0.5))
    figlegend.legend(bars, FIGURE_LABEL, prop=LEGEND_FP, \
                     loc=9,
                     bbox_to_anchor=(0, 0.4, 1, 1),
                     ncol=len(FIGURE_LABEL), mode="expand", shadow=False, \
                     frameon=False, handlelength=1.1, handletextpad=0.2, columnspacing=0.1)

    figlegend.savefig(FIGURE_FOLDER + '/' + filename + '.pdf')


# example for reading csv file
def ReadFile(tthread, batchInterval):
    # Creates a list containing w lists, each of h items, all set to 0
    w, h = 6, 5
    y = [[0 for x in range(w)] for y in range(h)]

    y_sum = [0 for x in range(w)]

    events = tthread * batchInterval

    gs_path = FILE_FOLER + '/GS/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(gs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][0] += float(breakdown_value[i + 1])
            y_sum[0] += float(breakdown_value[i + 1])

    bfs_path = FILE_FOLER + '/BFS/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(bfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][1] += float(breakdown_value[i + 1])
            y_sum[1] += float(breakdown_value[i + 1])

    dfs_path = FILE_FOLER + '/DFS/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(dfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][2] += float(breakdown_value[i + 1])
            y_sum[2] += float(breakdown_value[i + 1])

    op_gs_path = FILE_FOLER + '/OPGS/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(op_gs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][3] += float(breakdown_value[i + 1])
            y_sum[3] += float(breakdown_value[i + 1])

    op_bfs_path = FILE_FOLER + '/OPBFS/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(op_bfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][4] += float(breakdown_value[i + 1])
            y_sum[4] += float(breakdown_value[i + 1])

    op_dfs_path = FILE_FOLER + '/OPDFS/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(op_dfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][5] += float(breakdown_value[i + 1])
            y_sum[5] += float(breakdown_value[i + 1])

    for i in range(h):
        for j in range(w):
            if y_sum[j] != 0:
                y[i][j] = (y[i][j] / y_sum[j]) * 100

    print(y)

    return y


def ReadFileWithAbort(tthread, batchInterval):
    # Creates a list containing w lists, each of h items, all set to 0
    w, h = 6, 5
    y = [[0 for x in range(w)] for y in range(h)]

    y_sum = [0 for x in range(w)]

    events = tthread * batchInterval

    gs_path = FILE_FOLER + '/GSA/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(gs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][0] += float(breakdown_value[i + 1])
            y_sum[0] += float(breakdown_value[i + 1])

    bfs_path = FILE_FOLER + '/BFSA/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(bfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][1] += float(breakdown_value[i + 1])
            y_sum[1] += float(breakdown_value[i + 1])

    dfs_path = FILE_FOLER + '/DFSA/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(dfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][2] += float(breakdown_value[i + 1])
            y_sum[2] += float(breakdown_value[i + 1])

    op_gs_path = FILE_FOLER + '/OPGSA/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(op_gs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][3] += float(breakdown_value[i + 1])
            y_sum[3] += float(breakdown_value[i + 1])

    op_bfs_path = FILE_FOLER + '/OPBFSA/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(op_bfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][4] += float(breakdown_value[i + 1])
            y_sum[4] += float(breakdown_value[i + 1])

    op_dfs_path = FILE_FOLER + '/OPDFSA/threads = {}/totalEvents = {}'.format(tthread, events)
    lines = open(op_dfs_path).readlines()
    idx = locateIdx(lines)
    for line in lines[idx:]:
        breakdown_value = line.split("\t")
        print(breakdown_value)
        for i in range(0, 5):
            y[i][5] += float(breakdown_value[i + 1])
            y_sum[5] += float(breakdown_value[i + 1])

    for i in range(h):
        for j in range(w):
            if y_sum[j] != 0:
                y[i][j] = (y[i][j] / y_sum[j]) * 100

    print(y)

    return y


def locateIdx(lines):
    idx = 0
    for line in lines:
        idx += 1
        if line.startswith("SchedulerTimeBreakdownReport"):
            idx += 1
            break
    return idx


if __name__ == "__main__":
    # break into 5 parts
    legend_labels = ["Explore Time", "Next Time", "Useful Time", "Notify Time", "Construct Time"]
    for tthread in [1, 2, 4, 8, 16, 24]:
        for batchInterval in [2048]:
            x_values = ["GS", "BFS", "DFS", "OPGS", "OPBFS", "OPDFS"]
            y_values = ReadFile(tthread, batchInterval)
            DrawFigure(x_values, y_values, legend_labels,
                       '', 'percentage of time',
                       'breakdown_t{}_b{}'.format(tthread, batchInterval), True)
            x_values = ["GSA", "BFSA", "DFSA", "OPGSA", "OPBFSA", "OPDFSA"]
            y_values = ReadFileWithAbort(tthread, batchInterval)
            DrawFigure(x_values, y_values, legend_labels,
                       '', 'percentage of time',
                       'breakdown_with_abort_t{}_b{}'.format(tthread, batchInterval), True)
