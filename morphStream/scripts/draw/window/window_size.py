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
FILE_FOLER = '/home/myc/data/stats'


def ConvertEpsToPdf(dir_filename):
    os.system("epstopdf --outfile " + dir_filename + ".pdf " + dir_filename + ".eps")
    os.system("rm -rf " + dir_filename + ".eps")


def DrawFigure(x_values, y_values, legend_labels, x_label, y_label, filename, allow_legend):
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
                             bbox_to_anchor=(0.5, 1.2),
                             handletextpad=0.1,
                             borderaxespad=0.0,
                             handlelength=1.8,
                             labelspacing=0.3,
                             columnspacing=0.3,
                             )
            leg.get_frame().set_linewidth(2)
            leg.get_frame().set_edgecolor("black")

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


def ReadFileGS(x_axis, tthread, batchInterval, NUM_ITEMS, NUM_ACCESS, key_skewness, window_trigger_period, window_size, transaction_length, isCyclic, complexity):
    w, h = 1, len(x_axis)
    y = [[] for _ in range(w)]


    for window_size in x_axis:
        inputEvents = tthread * batchInterval
        op_gs_path = getPathGS("OP_NS", inputEvents, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, window_trigger_period, window_size, transaction_length, isCyclic, complexity)
        lines = open(op_gs_path).readlines()
        throughput = lines[0].split(": ")[1]
        y[0].append(float(throughput))

    print(y)

    return y


def getPathGS(algo, inputEvents, tthread, NUM_ITEMS, NUM_ACCESS, key_skewness, window_ratio, window_size, transaction_length, isCyclic, complexity):
    return FILE_FOLER + '/WindowedGrepSum/{}/threads = {}/totalEvents = {}/{}_{}_{}_{}_{}_{}_{}_{}'\
        .format(algo, tthread, inputEvents, NUM_ITEMS, 100, key_skewness, window_ratio, window_size, transaction_length, isCyclic, complexity)


if __name__ == '__main__':
    tthread = 24
    NUM_ITEMS = 3072
    NUM_ACCESS = 1
    deposit_ratio = 25
    key_skewness = 0
    window_trigger_period = 100
    window_size = 1024
    batchInterval = 10240
    isCyclic = "false"
    complexity = 10000
    transaction_length = 1

    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:n:k:o:a:b:c:m:t:")
    except getopt.GetoptError:
        print("Error")

    for opt, arg in opts:
        if opt in ['-i']:
            NUM_ITEMS = int(arg)
        elif opt in ['-d']:
            tthread = int(arg)
        elif opt in ['-n']:
            NUM_ACCESS = int(arg)
        elif opt in ['-k']:
            key_skewness = int(arg)
        elif opt in ['-o']:
            window_trigger_period = int(arg)
        elif opt in ['-a']:
            window_size = int(arg)
        elif opt in ['-b']:
            batchInterval = int(arg)
        elif opt in ['-t']:
            transaction_length = int(arg)
        elif opt in ['-c']:
            if int(arg) == 1:
                isCyclic = "true"
            else:
                isCyclic = "false"
        elif opt in ['-m']:
            complexity = int(arg)

    x_value = [1000, 10000, 100000]
    legend_labels = ["MorphStream"]
    # legend_labels = ["$GS_{OC}$", "$BFS_{OC}$", "$DFS_{OC}$", "$GS_{OP}$", "$BFS_{OP}$", "$DFS_{OP}$", "PAT"]
    legend = False
    y_axis = ReadFileGS(x_value, tthread, batchInterval, NUM_ITEMS, NUM_ACCESS, key_skewness, window_trigger_period, window_size, transaction_length, isCyclic, complexity)
    DrawFigure(x_value, y_axis, legend_labels, "Event Time Window Size", "Throughput(K/sec)", "window_size"
               .format(tthread, NUM_ITEMS, batchInterval, NUM_ACCESS, key_skewness, window_trigger_period, window_size, isCyclic, complexity),
               legend)