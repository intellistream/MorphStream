import csv
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
TICK_FONT_SIZE = 28
LABEL_FONT_SIZE = 28
LEGEND_FONT_SIZE = 30
LABEL_FP = FontProperties(style='normal', size=LABEL_FONT_SIZE)
LEGEND_FP = FontProperties(style='normal', size=LEGEND_FONT_SIZE)
TICK_FP = FontProperties(style='normal', size=TICK_FONT_SIZE)

MARKERS = (['o', 's', 'v', "^", "h", "v", ">", "x", "d", "<", "|", "", "|", "_"])
# you may want to change the color map for different figures
COLOR_MAP = ('#2874A6', '#F1C40F', '#239B56', '#a0d6df', '#7D3C98', '#AEB6BF', '#F5CBA7', '#82E0AA', '#AA4499')
# you may want to change the patterns for different figures
PATTERNS = (["\\", "/", "o", "|", "//", "\\\\", "//////", "//////", ".", "\\\\\\", "\\\\\\"])
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
# pylab.rcParams['text.usetex'] = True

# Sample data
categories = ['Construct', 'Explore', 'Useful', 'Sync', 'Lock', 'Others']
labels = ['CHC \n(5% new)', 'TransNFV \n(5% new)', 'CHC \n(60% new)', 'TransNFV \n(60% new)']
height = 0.5
tthread = 24


def draw_figure(values):
	# Plotting
	fig, ax = plt.subplots(figsize=(9, 5.3))
	# fig, ax = plt.subplots(figsize=(8, 5), left=0.2)


	# Transpose the input values to switch dimensions
	values = np.transpose(values)

	# Stacked bar chart
	bars = []
	left = np.zeros(len(labels))
	for i, category_values in enumerate(values):
		bar = ax.barh(labels, category_values, label=categories[i], left=left, height=height, 
					  hatch=PATTERNS[i], color=LINE_COLORS[i], edgecolor='black', linewidth=1)
		bars.append(bar)
		left += category_values
		
	# Specify the x-values for vertical lines
	vertical_lines_x = [100000, 200000, 300000, 400000]

	# Add vertical lines at specified x-values
	for line_x in vertical_lines_x:
		plt.axvline(x=line_x, color='black', linestyle='-', zorder=0)
		
	ax.set_xlabel('Execution Time (nsec)', fontproperties=LABEL_FP)
	ax.set_ylabel('', fontproperties=LABEL_FP)
	# Normalize x-axis labels by 100000
	# ax.set_xticklabels([int(label) // 100000 for label in ax.get_xticks()])
	# ax.set_xticks(np.arange(len(labels)))
	tick_values = ax.get_xticks()
	filtered_tick_values = [value for value in tick_values if value % 100000 == 0]
	ax.set_xticks(filtered_tick_values)
	ax.set_xticklabels([int(label) // 100000 for label in filtered_tick_values])
	ax.text(1.12, 0.08, r"$\times$10$^5$", transform=ax.transAxes, fontsize=LABEL_FONT_SIZE-4, va='top', ha='right')


	# ax.set_yticklabels(labels, rotation=90)

	# Legend
	# ax.legend(bbox_to_anchor=(0.55, 1.07), loc='upper center', ncol=3, prop=LEGEND_FP)

	# # Split the legend into two lines
	# # Legend
	# ax.legend(bbox_to_anchor=(0.55, 1.07), loc='upper center', ncol=3, bbox_transform=plt.gcf().transFigure, prop=LEGEND_FP)
	# ax.legend(bbox_to_anchor=(0.55, 1.02), loc='upper center', ncol=3, bbox_transform=plt.gcf().transFigure, prop=LEGEND_FP)
	legend = plt.legend(prop=LEGEND_FP,
                    loc='upper center',
                    ncol=3,
                    bbox_to_anchor=(0.5, 1.3),
                    shadow=False,
                    columnspacing=0.5,
                    frameon=True,
                    borderaxespad=0.0,
                    handlelength=1.5,
                    handletextpad=0.1,
                    labelspacing=0.1)

	legend.get_frame().set_linewidth(2)
	legend.get_frame().set_edgecolor('black')

	# Adjust the position of the legends
	# plt.subplots_adjust(top=0.9)

	# plt.tight_layout()

	# Display the chart
	# plt.show()
	plt.savefig("." + "/" + "breakdown_14400.pdf", bbox_inches='tight')



def read_csv_file(filename, start_line):
	data = []
	with open(filename, 'r') as file:
		csv_reader = csv.reader(file, delimiter='\t')
		for _ in range(start_line):
			next(csv_reader)  # Skip lines until the desired start line
		for line in csv_reader:
			cleaned_line = [cell.replace('\t', '').strip() for cell in line[1:]][0:-1]
			float_line = [float(x) for x in cleaned_line]
			data.append(float_line)
			# print(float_line)
	return data

def compute_avg_array(data):
	# Convert the data to a numpy array
	data_array = np.array(data)
	# Calculate the average value of each element along the first axis (axis=0)
	average_values = np.mean(data_array, axis=0)
	return average_values

def normalize_trans_values(data):
	array = np.zeros(6)
	array[0] = data[4] #construct
	array[1] = data[0] #schedule
	array[2] = data[2] #useful
	array[5] = data[1]+data[3] #others: TransNFV's next+notify
	# print("Trans", array)
	return array

def normalize_pat_values(data):
	array = np.zeros(6)
	array[3] = data[2] #sync
	array[4] = data[3] #lock
	array[2] = data[1] #useful
	array[5] = data[0] #others: PAT_indexings
	# print("CHC", array)
	return array


# Usage example
PAT_10_filename = '/Users/zhonghao/Downloads/LoadBalancer/PAT/threads = 24/totalEvents = 14400/14400_100_20_10_0_1_false_0_5_600'
PAT_60_filename = '/Users/zhonghao/Downloads/LoadBalancer/PAT/threads = 24/totalEvents = 14400/14400_100_20_10_0_1_false_0_60_600'
Trans_10_filename = '/Users/zhonghao/Downloads/LoadBalancer/OG_NS_A/threads = 24/totalEvents = 14400/14400_100_20_10_0_1_false_0_5_600'
Trans_60_filename = '/Users/zhonghao/Downloads/LoadBalancer/OG_NS_A/threads = 24/totalEvents = 14400/14400_100_20_10_0_1_false_0_60_600'
useless_lines = 7  # Replace with the line number from where you want to start reading

PAT_10_values = normalize_pat_values(compute_avg_array(read_csv_file(PAT_10_filename, tthread+useless_lines)))
PAT_60_values = normalize_pat_values(compute_avg_array(read_csv_file(PAT_60_filename, tthread+useless_lines)))
Trans_10_values = normalize_trans_values(compute_avg_array(read_csv_file(Trans_10_filename, tthread+useless_lines)))
Trans_60_values = normalize_trans_values(compute_avg_array(read_csv_file(Trans_60_filename, tthread+useless_lines)))

y_values = [PAT_10_values, Trans_10_values, PAT_60_values, Trans_60_values]

draw_figure(y_values)
